from typing import Union, List, Callable

from aiokafka import AIOKafkaProducer
from kafka.errors import KafkaConnectionError

from lib.connection import Producer


class AsyncKafkaProducer(Producer):

    def __init__(self, bootstrap_servers:Union[str, List[str]], value_serializer:Callable, **kwargs) -> None:
        self._bootstrap_servers: Union[str, List[str]] = bootstrap_servers
        self._value_serializer: Callable = value_serializer
        self._producer: AIOKafkaProducer = None
        super().__init__(**kwargs)

    async def connect(self) -> bool:
        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                value_serializer=self._value_serializer)
            await self._producer.start()
            return True
        except KafkaConnectionError:
            return False


    async def disconnect(self) -> None:
        if self._producer is not None:
            await self._producer.stop()

    async def send(self, data, **kwargs):
        await self._producer.send_and_wait(kwargs['topic'], data)
