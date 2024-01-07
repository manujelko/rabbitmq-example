import asyncio

from aio_pika import ExchangeType, IncomingMessage, connect


async def on_message(message: IncomingMessage):
    async with message.process():
        print(f" [x] {message.routing_key}: {message.body.decode()}")


async def main():
    connection = await connect("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("direct_logs", ExchangeType.DIRECT)
        queue = await channel.declare_queue(exclusive=True)
        severities = ["info"]
        for severity in severities:
            await queue.bind(exchange, routing_key=severity)
        await queue.consume(on_message)
        print(" [*] waiting for logs. To exit press CTRL+C")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
