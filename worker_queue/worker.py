import asyncio

from aio_pika import IncomingMessage, connect


async def on_message(message: IncomingMessage):
    async with message.process():
        task_body = message.body.decode()
        print(f" [x] Received {task_body}")
        # simulate work by sleeping for each dot in the message
        await asyncio.sleep(task_body.count("."))
        print(" [x] Done")


async def main():
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        channel = await connection.channel()
        # set prefetch count for fair dispatch
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue("task_queue", durable=True)
        await queue.consume(on_message)
        print(f" [*] Waiting for messages. To exit prcess CTRL+C")
        await asyncio.Future()  # run indefinetly


if __name__ == "__main__":
    asyncio.run(main())
