import asyncio
import logging
from contextlib import suppress
from inspect import getsourcelines, getfile
from typing import Union, List, Optional, Dict, Callable, Set

import numpy as np
from aiokafka import AIOKafkaConsumer
from kafka.errors import KafkaConnectionError

from lib.connection import Connection, ConnectionType

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -50s %(funcName) '
              '-40s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


class AsyncWorker:
    def __init__(
            self,
            consumer_topic: str,
            consumer_bootstrap_servers: Union[str, List[str]],
            consumer_group_id: str,
            value_deserializer: Callable,
            consumer_read_atomic: bool=True):

        self._consumer_topic: str = consumer_topic
        self._consumer_bootstrap_servers: Union[str, List[str]] = consumer_bootstrap_servers
        self._consumer_group_id: str = consumer_group_id
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._value_deserializer = value_deserializer
        self.consumer_reconnection_delay = 1
        self.consumer_reconnection_attempts = 10

        self.consumer_read_atomic = consumer_read_atomic

        self.agents: List[AsyncWorker._Agent] = []
        self._sinks: Dict[ConnectionType, Dict[str, Connection]] = {}

        self.loop = None

    def register_agent(self, agent):
        self.agents.append(agent)
        return len(self.agents) - 1

    def register_sink(self, sink:Connection):
        if self._sinks.get(sink.connection_type.name) is not None:
            self._sinks[sink.connection_type.name].update({sink.connection_id: sink})
        else:
            self._sinks.update({sink.connection_type.name: {sink.connection_id: sink}})

    async def _init_consumer(self) -> None:
        LOGGER.info('Initializing consumer')
        self._consumer = AIOKafkaConsumer(
            self._consumer_topic,
            bootstrap_servers=self._consumer_bootstrap_servers,
            group_id=self._consumer_group_id,
            value_deserializer=self._value_deserializer,
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            auto_offset_reset="earliest")

    async def _start_consumer(self) -> bool:
        while self.consumer_reconnection_attempts > 0:
            try:
                LOGGER.info('Connecting consumer to broker')
                await self._consumer.start()
                return True
            except KafkaConnectionError:
                await self._stop_consumer()
                LOGGER.info('Restarting Kafka Consumer after %d seconds. %d attempts left.',
                    self.consumer_reconnection_delay,
                    self.consumer_reconnection_attempts)
                await asyncio.sleep(self.consumer_reconnection_delay)
                self.consumer_reconnection_delay += 1
                self.consumer_reconnection_attempts -= 1
        return False

    async def _stop_consumer(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()

    async def _start_sinks(self) -> bool:
        for sink_type in ConnectionType:
            if self._sinks.get(sink_type.name) is not None:
                for sink in self._sinks.get(sink_type.name).values():
                    is_connected = await sink.connect()
                    if not is_connected:
                        return False
        return True

    def start_agents(self):
        for agent in self.agents:
            agent.start()

    async def stop_agents(self):
        for agent in self.agents:
            await agent.stop()

    async def _stop_sinks(self) -> None:
        for sink_type in ConnectionType:
            if self._sinks.get(sink_type.name) is not None:
                for sink in self._sinks.get(sink_type.name).values():
                    await sink.disconnect()

    async def read_batches(self):
        sink = self.agents[0].get_queue()
        async for message_chunk in self._consumer:
            await sink.put(message_chunk)

    async def read_atomic(self):
        sink = self.agents[0].get_queue()
        async for message_chunk in self._consumer:
            for message in message_chunk.value:
                await sink.put(message)

    async def start_worker(self) -> None:
        await self._init_consumer()
        is_conn_consumer = await self._start_consumer()
        is_conn_sinks = await self._start_sinks()
        self.start_agents()

        if is_conn_consumer or is_conn_sinks:
            if self.consumer_read_atomic:
                await self.read_atomic()
            else:
                await self.read_batches()

    async def stop_worker(self) -> None:
        await self._stop_consumer()
        await self.stop_agents()
        await self._stop_sinks()

    def run(self) -> None:
        self.loop = asyncio.get_event_loop()
        try:
            self.loop.run_until_complete(self.start_worker())
        except KeyboardInterrupt:
            LOGGER.info('Worker stopped manually.')
        finally:
            LOGGER.info('Stopping Worker.')
            self.loop.run_until_complete(self.stop_worker())

    async def send(self, producer_id, message, topic) -> None:
        try:
            return await self._sinks[ConnectionType.producer.name][producer_id].send(message, topic=topic)
        except AttributeError:
            LOGGER.exception('')
            await self.stop_worker()

    async def kvstore_batch_execute(self, db_connection_id, commands: List[str]):
        try:
            return await self._sinks[ConnectionType.kv_store.name][db_connection_id].batch_execute(commands)
        except AttributeError:
                LOGGER.exception('')
                await self.stop_worker()

    def agent(
            self,
            buffer_size:int=None,
            timeout:int=None,
            atomic_send:bool=False,
            cpu_intensive:bool=False,
            statistics:bool=False) -> Callable:
        """
        Decorator function for defining an agent. The decorated function acts as the
        processing logic and is passed as callback to the agent. The agent receives
        messages and dispatches them to the callback.
        :param buffer_size: The number of elements that are buffered in a batch before processing.
        :param timeout: The time in seconds before a batch is released for processing. Note, that
        the agent may stall incomplete batches for an indefinite amount of time, when not set.
        :param atomic_send: Flag for sending the elements in a batch individually to the next agent.
        :return: The wrapper function inside the decorator as a closure.
        """
        _agent = AsyncWorker._Agent(self, buffer_size, timeout, atomic_send, cpu_intensive, statistics)
        def wrapper(f):
            _agent.set_callback(f)
        return wrapper


    class _Agent:

        def __init__(
                self,
                worker,
                buffer_size:int=None,
                timeout:int=None,
                atomic_send:bool=False,
                cpu_intensive:bool=False,
                statistics:bool=False):
            self._worker: AsyncWorker = worker
            self._agent_index: int = self._worker.register_agent(self)

            self._consume_queue: asyncio.Queue = asyncio.Queue()
            self._callback: Callable = None
            self._sink: Optional[asyncio.Queue] = None
            self._task_references: Set[asyncio.Task] = set()

            self.buffer: Optional[asyncio.Queue] = None
            self._buffer_size: Optional[int] = buffer_size
            self.timeout: Optional[int] = timeout
            self.atomic_send: bool = atomic_send
            self.cpu_intensive = cpu_intensive
            self.statistics = statistics

        def start(self) -> None:
            LOGGER.info('Starting agent with id %s dispatching function %s (line %s in %s)',
                        self._agent_index,
                        self._callback.__name__,
                        getsourcelines(self._callback)[1],
                        getfile(self._callback).split('/')[-1])
            self._set_sink()
            if self._buffer_size is not None:
                self.buffer = asyncio.Queue(maxsize=self._buffer_size)
                self._task_references.add(asyncio.create_task(self._consume_buffered()))
            else:
                self._task_references.add(asyncio.create_task(self._consume()))

        async def stop(self) -> None:
            LOGGER.info('Stopping agent with id %s', self._agent_index)
            for task_reference in self._task_references:
                task_reference.cancel()
                with suppress(asyncio.CancelledError):
                    await task_reference

        def get_queue(self) -> asyncio.Queue:
            return self._consume_queue

        def set_callback(self, callback: Callable) -> None:
            self._callback = callback

        def _set_sink(self) -> None:
            if self._worker.agents[-1] == self:
                self._sink = None
            else:
                self._sink = self._worker.agents[self._agent_index + 1].get_queue()

        def _send(self, message) -> None:
            if self._sink is not None:
                if self.atomic_send is True:
                    if isinstance(message, dict):
                        iter = message.items()
                    else:
                        iter = message
                    [self._sink.put_nowait(msg) for msg in iter]
                else:
                    self._sink.put_nowait(message)


        async def _consume(self) -> None:
            while True:
                message = await self._consume_queue.get()
                if self.cpu_intensive is True:
                    await asyncio.sleep(0.5)
                processed = await self._callback(message)
                if self.statistics is True:
                    print(f'Size of queue from ({self._callback.__name__}): ', self._consume_queue.qsize())
                self._send(processed)

        def _clear_buffer(self):
            return np.array([self.buffer.get_nowait() for _ in range(self.buffer.qsize())])

        async def _consume_buffered(self) -> None:
            while True:
                try:
                    if self.timeout is not None:
                        message = await asyncio.wait_for(self._consume_queue.get(), timeout=self.timeout)
                    else:
                        message = await self._consume_queue.get()

                    if self.buffer.qsize() < self._buffer_size:
                        self.buffer.put_nowait(message)
                    else:
                        buffered = self._clear_buffer()
                        self.buffer.put_nowait(message)
                        processed = await self._callback(buffered)
                        self._send(processed)

                except asyncio.TimeoutError:
                    LOGGER.info('Timeout on agent.')
                    if self.buffer.qsize() > 0:
                        LOGGER.info('Releasing buffer.')
                        buffered = self._clear_buffer()
                        processed = await self._callback(buffered)
                        self._send(processed)
