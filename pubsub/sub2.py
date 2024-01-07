import asyncio

from aio_pika import ExchangeType, IncomingMessage, connect


async def on_message(message: IncomingMessage):
    async with message.process():
        print(f" [x] sub2 {message.body.decode()}")


async def main():
    connection = await connect("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("logs", ExchangeType.FANOUT)
        queue = await channel.declare_queue(exclusive=True)
        await queue.bind(exchange)
        await queue.consume(on_message)
        print(" [*] Waiting for logs. To exit press CTRL+C")
        await asyncio.Future()  # wait indefinitely


if __name__ == "__main__":
    asyncio.run(main())
