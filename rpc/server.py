import asyncio

from aio_pika import Channel, IncomingMessage, Message, connect


async def on_request(channel: Channel, message: IncomingMessage):
    n = int(message.body.decode())
    print(f" [.] fib({n})")
    response = fib(n)

    response_message = Message(
        body=str(response).encode(), correlation_id=message.correlation_id
    )

    await channel.default_exchange.publish(
        response_message, routing_key=message.reply_to
    )


def fib(n: int) -> int:
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


async def main() -> None:
    connection = await connect("amqp://guest:guest@localhost/")
    channel = await connection.channel()

    rpc_queue = await channel.declare_queue("rpc_queue")
    await rpc_queue.consume(
        lambda message: asyncio.create_task(on_request(channel, message))
    )

    print(" [x] Awaiting RPC requests")
    await asyncio.Future()  # run indefinitely


if __name__ == "__main__":
    asyncio.run(main())
