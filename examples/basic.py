import asyncio
import os
from datetime import datetime

from rf_event_listener.api import HttpEventsApi
from rf_event_listener.events import TypedMapEvent, EventVisitor, CommentPushedMapEvent
from rf_event_listener.listener import MapsListener, EventConsumer


MAP_ID = os.getenv('MAP_ID')

# Current user prefix (value of CurrentUserDto.kv_session)
USER_PREFIX = os.getenv('USER_PREFIX')


class Visitor(EventVisitor):
    async def comment_pushed(self, event: CommentPushedMapEvent):
        text = event.data['content']
        author = event.who.username
        print(f'New comment from {author}:\n{text}\n')


class Consumer(EventConsumer):
    def __init__(self, visitor: EventVisitor):
        self._visitor = visitor

    async def consume(self, timestamp: datetime, event: TypedMapEvent):
        await event.visit(self._visitor)

    # todo (optional) persist event offset to database
    async def commit(self, offset: str):
        print(f'Commit event offset: {offset}')


if __name__ == '__main__':
    visitor = Visitor(default_result=None)
    consumer = Consumer(visitor)

    api = HttpEventsApi()
    listener = MapsListener(api)

    # todo (optional) pass initial_offset from database
    listener.add_map(MAP_ID, USER_PREFIX, consumer, initial_offset=None)

    loop = asyncio.get_event_loop()
    loop.run_forever()
