import asyncio

from aio_pika import IncomingMessage, connect


async def on_message(message: IncomingMessage):
    async with message.process():
        print(f" [x] Received message: {message.body.decode()}")


async def main():
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        # create a channel
        channel = await connection.channel()

        # declare the queue
        queue = await channel.declare_queue("hello", durable=True)

        # start listening to the queue
        await queue.consume(on_message)

        print(" [*] Waiting for messages. To exit press CTRL-C")
        await asyncio.Future()  # run indefinetly


if __name__ == "__main__":
    asyncio.run(main())

