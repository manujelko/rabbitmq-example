import asyncio

from aio_pika import ExchangeType, Message, connect


async def main():
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("logs", ExchangeType.FANOUT)
        message_body = "info: Hello World!"
        message = Message(body=message_body.encode())
        await exchange.publish(message, routing_key="")
        print(" [x] Sent %r" % message_body)


if __name__ == "__main__":
    asyncio.run(main())
