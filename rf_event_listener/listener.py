import asyncio
import logging
from asyncio import Task, CancelledError, AbstractEventLoop
from datetime import datetime
from typing import Dict, Optional, Callable, Coroutine, Any, List

from pydantic import ValidationError

from rf_event_listener.api import EventsApi, KvEntry
from rf_event_listener.events import TypedMapEvent, CompoundMapEvent, any_event_to_typed, AnyMapEvent

logger = logging.getLogger('rf_maps_listener')


EventConsumerCallback = Callable[[datetime, TypedMapEvent], Coroutine[Any, Any, None]]


class EventConsumer:
    async def consume(self, timestamp: datetime, event: TypedMapEvent):
        raise NotImplementedError()

    async def commit(self, offset: str):
        pass

    async def close(self):
        pass


class MapsListener:
    def __init__(
            self,
            api: EventsApi,
            events_per_request: int = 100,
            loop: Optional[AbstractEventLoop] = None,
            skip_unknown_events: bool = False,
    ):
        self._api = api
        self._listeners: Dict[str, Task] = {}
        self._events_per_request = events_per_request
        self._loop = loop or asyncio.get_event_loop()
        self._skip_unknown_events = skip_unknown_events

    def add_map(
            self,
            map_id: str,
            kv_prefix: str,
            consumer: EventConsumer,
            initial_offset: Optional[str] = None
    ):
        if map_id in self._listeners:
            return
        listener = MapListener(
            self._api,
            consumer,
            self._events_per_request,
            map_id,
            kv_prefix,
            initial_offset,
            self._skip_unknown_events,
        )
        task = self._loop.create_task(listener.listen())
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
            consumer: EventConsumer,
            events_per_request: int,
            map_id: str,
            kv_prefix: str,
            offset: Optional[str],
            skip_unknown_events: bool,
    ):
        self._api = api
        self._consumer = consumer
        self._events_per_request = events_per_request
        self._map_id = map_id
        self._kv_prefix = kv_prefix
        self._offset = offset
        self._skip_unknown_events = skip_unknown_events

    async def listen(self):
        logger.info(f'[{self._map_id}] Map listener started')

        while True:
            try:
                await self._events_loop()
            except CancelledError:
                await self._consumer.close()
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
                offset = event.key[-1]
                await process_event(self._map_id, self._consumer.consume, event, self._skip_unknown_events)
                await self._consumer.commit(offset)
                self._offset = offset
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


async def process_event(
        map_id: str,
        consume: EventConsumerCallback,
        event: KvEntry,
        skip_unknown_events: bool,
):
    logger.debug(f"[{map_id}] Processing event {event}")

    try:
        offset = event.key[-1]
        timestamp = datetime.utcfromtimestamp(int(offset) / 1000)
        events = parse_compound_event(map_id, event.value, skip_unknown_events)
    except (ValidationError, ValueError, IndexError):
        if skip_unknown_events:
            logger.exception(f"[{map_id}] Error in event parsing, event = {event}")
            return
        raise

    try:
        for event in events:
            await consume(timestamp, event)
    except CancelledError:
        raise
    except Exception:
        logger.exception(f"[{map_id}] Error in event processing")


def parse_compound_event(map_id: str, json: dict, skip_unknown_events=False) -> List[TypedMapEvent]:
    event = CompoundMapEvent(**json)
    additional = event.additional or []

    result = [any_event_to_typed(event)]

    for e in additional:
        json = dict(**e)
        json['who'] = event.who
        try:
            parsed = AnyMapEvent(**json)
            result.append(any_event_to_typed(parsed))
        except ValidationError:
            if not skip_unknown_events:
                raise
            logger.exception(f"[{map_id}] Error in event parsing, event = {event}")

    return result
