import pytest
from pydantic import ValidationError

from rf_event_listener.events import AnyMapEvent, EventType, NodeUpdatedMapEvent, any_event_to_typed, \
    SearchQuerySavedMapEvent, SearchQuerySavedData, NodeCreatedMapEvent, NodeDeletedMapEvent, parse_compound_event, \
    event_type_to_typed_event


def test_simple_event():
    json = {
        'type': 'node_updated',
        'what': 'node-id',
        'sessionId': 'test-session',
    }

    expected_any_event = AnyMapEvent(
        type=EventType.node_updated,
        what='node-id',
        session_id='test-session',
    )
    actual_any_event = AnyMapEvent(**json)
    assert expected_any_event == actual_any_event

    expected_typed_event = NodeUpdatedMapEvent(
        type=EventType.node_updated,
        what='node-id',
        session_id='test-session',
    )
    actual_typed_event = any_event_to_typed(actual_any_event)
    assert expected_typed_event == actual_typed_event


def test_event_with_data():
    json = {
        'type': 'search_query_saved',
        'what': 'node-id',
        'data': {
            'id': 'search-id',
            'title': 'Foo',
            'query': 'search query',
            'timestamp': 123,
            'user_id': 'user-id',
        }
    }

    expected_any_event = AnyMapEvent(
        type=EventType.search_query_saved,
        what='node-id',
        session_id=None,
        data=json['data'],
    )
    actual_any_event = AnyMapEvent(**json)
    assert expected_any_event == actual_any_event

    expected_typed_event = SearchQuerySavedMapEvent(
        type=EventType.search_query_saved,
        what='node-id',
        session_id=None,
        data=SearchQuerySavedData(
            id='search-id',
            title='Foo',
            query='search query',
            timestamp=123,
            user_id='user-id',
        ),
    )
    actual_typed_event = any_event_to_typed(actual_any_event)
    assert expected_typed_event == actual_typed_event


def test_parse_compound_event():
    json = {
        'type': 'node_updated',
        'what': 'node-id',
        'sessionId': 'test-session',
        'additional': [
            {
                'type': 'node_created',
                'what': 'node-id-2',
                'sessionId': 'test-session-2',
            },
            {
                'type': 'node_deleted',
                'what': 'node-id-3',
            },
        ],
    }

    expected = [
        NodeUpdatedMapEvent(type=EventType.node_updated, what='node-id', session_id='test-session'),
        NodeCreatedMapEvent(type=EventType.node_created, what='node-id-2', session_id='test-session-2'),
        NodeDeletedMapEvent(type=EventType.node_deleted, what='node-id-3', session_id=None),
    ]
    actual = parse_compound_event(json)
    assert expected == actual


def test_parse_empty_compound_event():
    json = {
        'type': 'node_updated',
        'what': 'node-id',
        'sessionId': 'test-session',
    }

    expected = [
        NodeUpdatedMapEvent(type=EventType.node_updated, what='node-id', session_id='test-session'),
    ]
    actual = parse_compound_event(json)
    assert expected == actual


def test_all_events_are_in_mapping_dict():
    assert [*EventType] == list(event_type_to_typed_event.keys())


def test_parse_unknown_event():
    json = {
        'type': 'unknown_event',
        'what': 'node-id',
        'sessionId': 'test-session',
    }

    with pytest.raises(ValidationError):
        AnyMapEvent(**json)
