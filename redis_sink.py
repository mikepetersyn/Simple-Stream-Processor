from lib.connection import KVStore
import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool

class AsyncRedis(KVStore):

    def __init__(self, connection_url:str, **kwargs):
        self._connection_url: str = connection_url
        self._connection_pool: ConnectionPool = None
        super().__init__(**kwargs)

    async def connect(self) -> bool:
        try:
            self._connection_pool = await redis.from_url(self._connection_url)
            return True
        except Exception:
            return False


    async def disconnect(self):
        if self._connection_pool is not None:
            await self._connection_pool.close()


    async def put(self, key, value, **kwargs):
        async with self._connection_pool.pipeline(transaction=True) as pipe:
            response = []
            for n, k, v in zip(kwargs['names'], key, value):
                pipe.hsetnx(name=n, key=k, value=v)
                response.extend(await pipe.execute())
        return response

    async def batch_execute(self, commands):
        async with self._connection_pool.pipeline(transaction=True) as pipe:
            response = []
            for c in commands:
                pipe.execute_command(*c)
                response.extend(await pipe.execute())
        return response

