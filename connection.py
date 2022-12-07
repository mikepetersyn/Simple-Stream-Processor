from abc import ABC, abstractmethod
from enum import Enum


class ConnectionType(Enum):
    consumer = 1
    producer = 2
    kv_store = 3

class Connection(ABC):

    def __init__(self, connection_id:str, connection_type:ConnectionType, **kwargs) -> None:
        self.connection_id:str = connection_id
        self.connection_type:ConnectionType = connection_type
        super().__init__(**kwargs)

    @abstractmethod
    async def connect(self) -> bool:
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        pass


class Producer(Connection):

    def __init__(self, **kwargs):
        super().__init__(connection_type=ConnectionType.producer, **kwargs)

    @abstractmethod
    async def send(self, data, **kwargs):
        pass


class KVStore(Connection):

    def __init__(self, **kwargs):
        super().__init__(connection_type=ConnectionType.kv_store, **kwargs)

    @abstractmethod
    async def put(self, key, value, **kwargs):
        pass
