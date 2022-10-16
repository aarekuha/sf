import sys
import asyncio
from aio_pika import Message, connect


async def send(text: str | None) -> None:
    connection = await connect("amqp://guest:guest@localhost:5672/")
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("hello", auto_delete=False)
        if text:
            await channel.default_exchange.publish(
                Message(
                    body=text.encode(),
                    headers={
                        "route": "train_model",
                        "model_id": 17,
                    },
                ),
                routing_key=queue.name,
            )
        else:
            await channel.default_exchange.publish(
                Message(
                    body="Without content".encode(),
                    headers={
                        "route": "another_method",
                        "one": "any string",
                        "more": "another string",
                    },
                ),
                routing_key=queue.name,
            )
        print(f"Published: {text}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        asyncio.run(send(text=sys.argv[1]))
    else:
        asyncio.run(send(text=None))
