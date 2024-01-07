import asyncio

from aio_pika import ExchangeType, Message, connect


async def main():
    connection = await connect("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("topic_logs", ExchangeType.TOPIC)
        routing_keys = ["kern.critical", "auth.info", "cron.warning"]
        for routing_key in routing_keys:
            message_body = f"{routing_key}: Hello World!"
            message = Message(body=message_body.encode())
            await exchange.publish(message, routing_key=routing_key)
            print(f" [x] Sent {message_body}")


if __name__ == "__main__":
    asyncio.run(main())
