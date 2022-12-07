# Simple-Stream-Processor

The simple stream processor is a lightweight library for the implementation of stream processing applications. Inspired by the [Faust](https://github.com/robinhood/faust)-Framework, it is based on the definition of **workers** that call **agent functions**, which in turn encapsulate the processing logic. Every worker receives data from Kafka topics and sends processed data into a **sink**.

## Quick Start

First define a worker from the class `AsyncWorker` as follows:

```
worker = AsyncWorker(
    consumer_topic=config.CONSUMER_TOPIC,
    consumer_bootstrap_servers=config.CONSUMER_BOOTSTRAP_SERVERS,
    consumer_group_id=config.CONSUMER_GROUP_ID)
```

As a standard, every worker has to define a connection to a Kafka server and a topic as it ingests data from it. Next, define a sink that receives the processed data from the worker. Currently, only Kafka topics and redis are available as sinks.

```
producer_sink = AsyncKafkaProducer(
    connection_id=config.PRODUCER_ID,
    bootstrap_servers=config.PRODUCER_BOOTSTRAP_SERVERS)
```

```
redis_sink = AsyncRedis(
    connection_id=config.REDIS_ID,
    connection_url=config.REDIS_CONNECTION)
```

Next, register the sinks by the worker instance.

```
worker.register_sink(producer_sink)
worker.register_sink(redis_sink)
```

An `agent` decorator function encapsulates the processing logic and is passed as callback to the worker. The worker receives messages and dispatches them to the callback. By defining multiple agents, messages are processed consecutively by each callback. By defining an agent as shown below, the respective callback is inserted into a list of the respective worker. Thus, the order in which the agents are defined also define the order in which the operations are applied on the data.

```
@worker.agent()
async def hash_dataset_chunk(X) -> Tuple[np.ndarray, np.ndarray]:
    y_pred = foo(dataset_chunk)
    return X, y_pred
```

Workers are flexible. For instance, a worker can define different bootstrap servers for the consumer and a producer sink. In fact, sinks can be placed arbitrarily in the processing logic. If an agent should send its data to a next agent, two requirements must be met. First, the agent must return the respective data. Second, the next agent must be defined appropriately and define the data to receive in the function arguments.

```
@worker.agent()
async def send_to_producer_and_next_agent(data) -> None:
    data = bar(data)
    await worker.send(
        producer_id=config.PRODUCER_ID,
        topic=config.PRODUCER_TOPIC,
        message=data)
        return data

@worker.agent()
async def send_to_another_sink(data) -> None:
    data = foobar()
    await worker.send(
        producer_id=config.PRODUCER_ID_2,
        topic=config.PRODUCER_TOPIC_2,
        message=data)
```

The decorator accepts different keyword arguments that configurate the behaviour of the respective agent.

```
buffer_size:int=10000 # buffers 10000 messages before starting to process them in a batch
timeout:int=15 # if a buffer_size is defined, release the buffer after 15 seconds regardless of the amount of messages that have been collected
atomic_send:bool=False # if messages were processed in a batch, send them to the next agent individually
```

In order to start the worker, call `run()`. As workers are communicating over Kafka, a scalable deployment of your streaming application with a dynamic number of workers is conceivable.

```
if __name__ == '__main__':
    worker.run()
```

