import asyncio
import logging
from asyncio import Task, create_task, CancelledError
from datetime import datetime
from typing import Dict, Optional, Callable, Coroutine, Any

from rf_event_listener.api import EventsApi, KvEntry
from rf_event_listener.events import parse_compound_event, TypedMapEvent

logger = logging.getLogger('rf_maps_listener')


MapListenerCallback = Callable[[datetime, TypedMapEvent], Coroutine[Any, Any, None]]


class MapsListener:
    def __init__(self, api: EventsApi, events_per_request: int = 100):
        self._api = api
        self._listeners: Dict[str, Task] = {}
        self._events_per_request = events_per_request

    def add_map(self, map_id: str, kv_prefix: str, listener: MapListenerCallback, initial_offset: Optional[str] = None):
        if map_id in self._listeners:
            return
        listener = MapListener(self._api, listener, self._events_per_request, map_id, kv_prefix, initial_offset)
        task = create_task(listener.listen())
        self._listeners[map_id] = task

    def remove_map(self, map_id: str):
        task = self._listeners.get(map_id, None)
        if task is None:
            return
        task.cancel()
        del self._listeners[map_id]


class MapListener:
    def __init__(
            self,
            api: EventsApi,
            listener: MapListenerCallback,
            events_per_request: int,
            map_id: str,
            kv_prefix: str,
            offset: Optional[str]
    ):
        self._api = api
        self._listener = listener
        self._events_per_request = events_per_request
        self._map_id = map_id
        self._kv_prefix = kv_prefix
        self._offset = offset

    async def listen(self):
        logger.info(f'[{self._map_id}] Map listener started')

        while True:
            try:
                await self._events_loop()
            except CancelledError:
                break
            except Exception:
                # todo exp. timeout
                logger.exception(f"[{self._map_id}] Error in events loop")
                await asyncio.sleep(60)

        logger.info(f"[{self._map_id}] Map listener stopped")

    async def _events_loop(self):
        logger.info(f"[{self._map_id}] Initial kv offset = {self._offset}")

        notify_last = await self._api.get_map_notify_last(self._map_id, self._kv_prefix)
        self._offset = self._offset or notify_last.value
        logger.info(f"[{self._map_id}] Initial notify last version = {notify_last.version}")

        while True:
            events = await self._api.get_map_notify(
                self._map_id, self._kv_prefix, self._offset, self._events_per_request
            )
            if len(events) != 0:
                logger.info(f"[{self._map_id}] Read {len(events)} events")
            for event in events:
                await self._process_event(event)
                self._offset = event.key[-1]
                logger.info(f"[{self._map_id}] New KV offset = {self._offset}")
            if len(events) < self._events_per_request:
                new_notify_last = await self._api.wait_for_map_notify_last(
                    self._map_id,
                    self._kv_prefix,
                    notify_last.version
                )
                if new_notify_last is not None:
                    logger.info(f"[{self._map_id}] New notify last version = {new_notify_last.version}")
                    notify_last = new_notify_last

    async def _process_event(self, event: KvEntry):
        try:
            logger.debug(f"[{self._map_id}] Processing event {event}")
            timestamp = datetime.utcfromtimestamp(int(event.key[-1]) / 1000)
            events = parse_compound_event(event.value)
            for event in events:
                await self._listener(timestamp, event)
        except CancelledError:
            raise
        except Exception:
            logger.exception(f"[{self._map_id}] Error in event processing")
