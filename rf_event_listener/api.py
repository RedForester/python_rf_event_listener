from typing import Optional, List

from rf_event_listener.events import BaseEventModel, CompoundMapEvent


class KvNotifyLast(BaseEventModel):
    value: Optional[str]
    version: str


class KvEntry(BaseEventModel):
    key: List[str]
    value: CompoundMapEvent


class EventsApi:
    async def get_map_notify_last(self, map_id: str, kv_prefix: str) -> KvNotifyLast:
        raise NotImplementedError()

    async def get_map_notify(self, map_id: str, kv_prefix: str, offset: Optional[str], limit: int) -> List[KvEntry]:
        raise NotImplementedError()

    async def wait_for_map_notify_last(self, map_id: str, kv_prefix: str, wait_version: str) -> Optional[KvNotifyLast]:
        raise NotImplementedError()
