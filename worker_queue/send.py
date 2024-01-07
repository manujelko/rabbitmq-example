import asyncio

from aio_pika import Message, connect


async def main():
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("task_queue", durable=True)
        for i in range(10):
            message_body = f"Task {i}{i*'.'}"
            message = Message(
                body=message_body.encode(),
                delivery_mode=2,  # persistent message
            )
            await channel.default_exchange.publish(message, routing_key=queue.name)
            print(f" [x] Sent {message_body}")


if __name__ == "__main__":
    asyncio.run(main())
