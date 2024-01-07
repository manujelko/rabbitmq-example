import asyncio
import uuid

from aio_pika import Channel, Connection, IncomingMessage, Message, Queue, connect


class FibonacciRpcClient:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.channel: Channel | None = None
        self.callback_queue: Queue | None = None
        self.futures: dict[str, asyncio.Future] = {}

    async def _setup(self):
        if not self.channel or not self.callback_queue:
            self.channel = await self.connection.channel()
            self.callback_queue = await self.channel.declare_queue(exclusive=True)
            await self.callback_queue.consume(self.on_response)

    def on_response(self, message: IncomingMessage):
        future = self.futures.pop(message.correlation_id)
        future.set_result(int(message.body.decode()))

    async def fibonacci(self, n: int) -> int:
        await self._setup()

        correlation_id = str(uuid.uuid4())
        future = asyncio.Future()
        self.futures[correlation_id] = future

        await self.channel.default_exchange.publish(
            Message(
                body=str(n).encode(),
                correlation_id=correlation_id,
                reply_to=self.callback_queue.name,
            ),
            routing_key="rpc_queue",
        )
        return await future


async def main() -> None:
    connection = await connect("amqp://guest:guest@localhost/")
    client = FibonacciRpcClient(connection)
    print(" [x] Requesting fib(30)")
    response = await client.fibonacci(30)
    print(f" [.] Got {response}")

    await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
