import asyncio

from aio_pika import ExchangeType, Message, connect


async def main():
    connection = await connect("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("direct_logs", ExchangeType.DIRECT)
        severities = ["info", "warning", "error"]
        for severity in severities:
            message_body = f"{severity}: Hello World!"
            message = Message(body=message_body.encode())
            await exchange.publish(message, routing_key=severity)
            print(f" [x] Sent {message_body}")


if __name__ == "__main__":
    asyncio.run(main())
