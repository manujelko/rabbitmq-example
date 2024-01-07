import asyncio

from aio_pika import Message, connect


async def main():
    connection = await connect("amqp://guest:guest@localhost")

    async with connection:
        # create a channel
        channel = await connection.channel()

        # declare a queue
        queue = await channel.declare_queue("hello", durable=True)

        # send a message
        await channel.default_exchange.publish(
            Message(body="Hello, World!".encode()),
            routing_key=queue.name,
        )

        print(" [x] Sent 'Hello, World!'")


if __name__ == "__main__":
    asyncio.run(main())
