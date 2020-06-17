import pytest

from rf_event_listener.events import NodeUpdatedMapEvent, EventType, EventVisitor, NodeDeletedMapEvent


class MockVisitor(EventVisitor[str]):
    def node_updated(self, event: 'NodeUpdatedMapEvent') -> str:
        assert event.what == 'node-id'
        return 'event_accepted'


@pytest.mark.asyncio
async def test_visitor_accepts_events():
    event = NodeUpdatedMapEvent(
        type=EventType.node_updated,
        what='node-id'
    )
    visitor = MockVisitor('default')
    result = await event.visit(visitor)
    assert result == 'event_accepted'


@pytest.mark.asyncio
async def test_visitor_uses_default_result():
    event = NodeDeletedMapEvent(
        type=EventType.node_deleted,
        what='node-id'
    )
    visitor = MockVisitor('default')
    result = await event.visit(visitor)
    assert result == 'default'
