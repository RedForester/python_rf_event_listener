import asyncio
from asyncio import Future, wait_for
from datetime import datetime
from typing import Optional, List, Tuple

import pytest

from rf_event_listener.api import EventsApi, KvNotifyLast, KvEntry
from rf_event_listener.events import TypedMapEvent, CompoundMapEvent, EventType, MapEventUser, NodeUpdatedMapEvent, \
    NodeDeletedMapEvent, any_event_to_typed
from rf_event_listener.listener import MapsListener, process_event, EventConsumer


class MockEventsApi(EventsApi):
    def __init__(self, events: List[KvEntry], map_id: str, kv_prefix: str):
        self._events = [*events]
        self._map_id = map_id
        self._kv_prefix = kv_prefix
        self._waiter = Future()
        self._drain = Future()

    def push_event(self, entry: KvEntry):
        self._events.append(entry)
        self._waiter.set_result(None)
        self._waiter = Future()

    async def wait_for_drain(self):
        await self._drain

    async def get_map_notify_last(self, map_id: str, kv_prefix: str) -> KvNotifyLast:
        assert self._map_id == map_id
        assert self._kv_prefix == kv_prefix

        value = None
        if len(self._events) > 0:
            value = self._events[-1].key[-1]

        return KvNotifyLast(value=value, version=str(len(self._events)))

    async def get_map_notify(self, map_id: str, kv_prefix: str, offset: Optional[str], limit: int) -> List[KvEntry]:
        offset = offset or ''
        selected_events = [e for e in self._events if e.key[-1] > offset]
        return selected_events[:limit]

    async def wait_for_map_notify_last(self, map_id: str, kv_prefix: str, wait_version: str) -> Optional[KvNotifyLast]:
        notify_last = await self.get_map_notify_last(map_id, kv_prefix)
        assert wait_version == notify_last.version
        self._drain.set_result(None)
        self._drain = Future()
        await self._waiter
        return await self.get_map_notify_last(map_id, kv_prefix)


@pytest.mark.asyncio
async def test_initially_empty_event_queue():
    completed: Future[Tuple[datetime, TypedMapEvent]] = Future()

    async def consumer(timestamp: datetime, actual_event: TypedMapEvent):
        completed.set_result((timestamp, actual_event))

    api = MockEventsApi(
        events=[],
        map_id='map-id',
        kv_prefix='map-prefix',
    )
    listener = MapsListener(api)
    listener.add_map('map-id', 'map-prefix', consumer, None)

    await api.wait_for_drain()

    expected_event = NodeUpdatedMapEvent(
        type=EventType.node_updated,
        who=MapEventUser(
            id='user-id',
            username='user@test',
        ),
        what='node-id',
        session_id=None
    )
    api.push_event(
        KvEntry(key=['0'], value=expected_event.dict())
    )

    result = await wait_for(completed, 10)
    assert result == (datetime.utcfromtimestamp(0), expected_event)
    listener.remove_map('map-id')


@pytest.mark.asyncio
async def test_filled_event_queue():
    completed: Future[Tuple[datetime, TypedMapEvent]] = Future()

    async def consumer(timestamp: datetime, actual_event: TypedMapEvent):
        completed.set_result((timestamp, actual_event))

    old_event = CompoundMapEvent(
        type=EventType.node_updated,
        who=MapEventUser(
            id='user-id',
            username='user@test',
        ),
        what='node-id',
        session_id=None
    ).dict()

    api = MockEventsApi(
        events=[
            KvEntry(key=['0'], value=old_event),
            KvEntry(key=['1'], value=old_event),
            KvEntry(key=['2'], value=old_event),
        ],
        map_id='map-id',
        kv_prefix='map-prefix',
    )
    listener = MapsListener(api)
    listener.add_map('map-id', 'map-prefix', consumer, None)

    await api.wait_for_drain()

    expected_event = NodeUpdatedMapEvent(
        type=EventType.node_updated,
        who=MapEventUser(
            id='user-id',
            username='user@test',
        ),
        what='node-id',
        session_id=None
    )
    api.push_event(
        KvEntry(key=['3'], value=expected_event.dict())
    )

    result = await wait_for(completed, 10)
    assert result == (datetime.utcfromtimestamp(0.003), expected_event)
    listener.remove_map('map-id')


@pytest.mark.asyncio
async def test_filled_event_queue_with_offset():
    completed: Future[Tuple[datetime, TypedMapEvent]] = Future()

    async def consumer(timestamp: datetime, actual_event: TypedMapEvent):
        completed.set_result((timestamp, actual_event))

    old_event = CompoundMapEvent(
        type=EventType.node_updated,
        who=MapEventUser(
            id='user-id',
            username='user@test',
        ),
        what='node-id',
        session_id=None
    ).dict()

    api = MockEventsApi(
        events=[
            KvEntry(key=['0'], value=old_event),
            KvEntry(key=['1'], value=old_event),
            KvEntry(key=['2'], value=old_event),
        ],
        map_id='map-id',
        kv_prefix='map-prefix',
    )
    listener = MapsListener(api)
    listener.add_map('map-id', 'map-prefix', consumer, '1')

    api.push_event(
        KvEntry(key=['2'], value=old_event)
    )

    result = await wait_for(completed, 10)
    assert result == (datetime.utcfromtimestamp(0.002), NodeUpdatedMapEvent(**old_event))
    listener.remove_map('map-id')


@pytest.mark.asyncio
async def test_skip_unknown_event():
    processed_events = []

    async def consumer(_: datetime, actual_event: TypedMapEvent):
        processed_events.append(actual_event)

    event = KvEntry(
        key=['0'],
        value={
            'type': 'unknown_event',
        },
    )

    await process_event(
        map_id='map',
        listener=consumer,
        event=event,
        skip_unknown_events=True
    )

    assert processed_events == []


@pytest.mark.asyncio
async def test_skip_unknown_event_inside_compound():
    processed_events = []

    async def consumer(_: datetime, actual_event: TypedMapEvent):
        processed_events.append(actual_event)

    who = MapEventUser(
        id='user',
        username='user@test',
    )

    first_event = NodeUpdatedMapEvent(
        type=EventType.node_updated,
        what='first',
        who=who,
    )

    unknown_event = {
        'type': 'unknown_event',
    }

    second_event = NodeDeletedMapEvent(
        type=EventType.node_deleted,
        what='second',
        who=who,
    )

    wrapper_event = CompoundMapEvent(
        type=EventType.node_created,
        what='root',
        who=who,
        additional=[
            first_event.dict(),
            unknown_event,
            second_event.dict(),
        ],
    )

    event = KvEntry(
        key=['0'],
        value=wrapper_event.dict(),
    )

    await process_event(
        map_id='map',
        listener=consumer,
        event=event,
        skip_unknown_events=True
    )

    expected_events = [
        any_event_to_typed(wrapper_event),
        first_event,
        second_event,
    ]

    assert expected_events == processed_events


@pytest.mark.asyncio
async def test_close_event_consumer():
    completed: Future[None] = Future()

    class CloseableConsumer(EventConsumer):
        async def consume(self, timestamp: datetime, event: TypedMapEvent):
            pass

        async def close(self):
            await asyncio.sleep(0.001)
            completed.set_result(None)

    api = MockEventsApi(
        events=[],
        map_id='map-id',
        kv_prefix='map-prefix',
    )
    listener = MapsListener(api)
    listener.add_map('map-id', 'map-prefix', CloseableConsumer())

    await asyncio.sleep(0.001)

    listener.remove_map('map-id')
    await wait_for(completed, 10)


# todo tests
#  error in consumer,
#  timeout in wait_for_notify_last,
#  remove_map
#  read more events than limit

