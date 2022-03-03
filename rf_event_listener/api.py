import aiohttp
import asyncio
from aiohttp import ClientSession
from typing import Optional, List
from yarl import URL

from rf_event_listener.events import BaseEventModel


DEFAULT_RF_URL = URL('https://app.redforester.com')


class KvNotifyLast(BaseEventModel):
    value: Optional[str]
    version: str


class KvEntry(BaseEventModel):
    key: List[str]
    value: dict


class EventsApi:
    async def get_map_notify_last(self, map_id: str, kv_prefix: str) -> KvNotifyLast:
        raise NotImplementedError()

    async def get_map_notify(self, map_id: str, kv_prefix: str, offset: Optional[str], limit: int) -> List[KvEntry]:
        raise NotImplementedError()

    async def wait_for_map_notify_last(self, map_id: str, kv_prefix: str, wait_version: str) -> Optional[KvNotifyLast]:
        raise NotImplementedError()


class HttpEventsApi(EventsApi):
    def __init__(self, base_url: URL = DEFAULT_RF_URL, read_timeout: float = 60):
        self._base_url = base_url
        self._read_timeout = read_timeout
        self._session = ClientSession(
            read_timeout=60,
            raise_for_status=True
        )

    async def __aenter__(self) -> 'HttpEventsApi':
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.__aexit__(exc_type, exc_val, exc_tb)

    async def close_session(self):
        """ Only if you using HttpEventsApi instance without context manager """
        await self._session.close()

    async def get_map_notify_last(self, map_id: str, kv_prefix: str) -> KvNotifyLast:
        url = self._base_url / f"kv/keys/mapNotifLast:{map_id}:{kv_prefix}"
        async with self._session.get(url) as resp:
            body = await resp.json()
            return KvNotifyLast(**body)

    async def get_map_notify(self, map_id: str, kv_prefix: str, offset: Optional[str], limit: int) -> List[KvEntry]:
        url = self._base_url / f"kv/partition/mapNotif:{map_id}:{kv_prefix}"
        query = {'limit': limit}
        if offset is not None:
            query['from'] = offset
        url = url.with_query(query)

        async with self._session.get(url) as resp:
            body = list(await resp.json())
            return [KvEntry(**e) for e in body]

    async def wait_for_map_notify_last(self, map_id: str, kv_prefix: str, wait_version: str) -> Optional[KvNotifyLast]:
        try:
            url = self._base_url / f"kv/keys/mapNotifLast:{map_id}:{kv_prefix}"
            url = url.with_query({
                'waitVersion': wait_version,
                'waitTimeout': self._read_timeout,
            })
            async with self._session.get(url) as resp:
                body = await resp.json()
                return KvNotifyLast(**body)
        except asyncio.TimeoutError:
            return None
        except aiohttp.ClientResponseError as e:
            if e.status == 408:
                return None
            raise e
