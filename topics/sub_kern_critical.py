import asyncio

from aio_pika import ExchangeType, IncomingMessage, connect


async def on_message(message: IncomingMessage):
    async with message.process():
        print(f" [x] {message.routing_key}: {message.body.decode()}")


async def main():
    connection = await connect("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("topic_logs", ExchangeType.TOPIC)
        queue = await channel.declare_queue(exclusive=True)
        binding_keys = ["kern.critical"]
        for binding_key in binding_keys:
            await queue.bind(exchange, routing_key=binding_key)
        await queue.consume(on_message)
        print(" [*] Waiting for logs. To exit press CTRL+C")
        await asyncio.Future()  #  run indefinitely


if __name__ == "__main__":
    asyncio.run(main())
