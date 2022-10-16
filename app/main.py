import asyncio
import logging

from services import Queue
from services import WorkerApp
from models import QueuedResponse
from modules import Config

config: Config = Config()
queue: Queue = Queue(
    src_queue_name=config.SRC_QUEUE_NAME,
    src_queue_host=config.SRC_QUEUE_HOST,
    src_queue_port=config.SRC_QUEUE_PORT,
    src_queue_username=config.SRC_QUEUE_USERNAME,
    src_queue_password=config.SRC_QUEUE_PASSWORD,
    dst_queue_name=config.DST_QUEUE_NAME,
    dst_queue_host=config.DST_QUEUE_HOST,
    dst_queue_port=config.DST_QUEUE_PORT,
    dst_queue_username=config.DST_QUEUE_USERNAME,
    dst_queue_password=config.DST_QUEUE_PASSWORD,
)
app: WorkerApp = WorkerApp(queue=queue)
logger = logging.getLogger("main.py")


@app.post(params="train_model.{model_id}")
async def do_smth(body, model_id) -> QueuedResponse:
    """
        Пример: функция ожидает в очереди сообщение с заголовком
                "route=train_model", обработывает body сообщения и
                заголовок model_id. После обработки результат
                складывается в очередь с результатами.
    """
    logger.info(f"do_smth: {body=}, {model_id=}")
    # This result will be putted in queue
    return QueuedResponse("Yep! =)")


@app.post(params="train_model.{model_id}")
async def do_smth2(body: str, model_id: int) -> str:
    """
        Пример: функция ожидает в очереди сообщение с заголовком
                "route=train_model", обработывает body сообщения и
                заголовок model_id. После обработки результат
                НЕ складывается в очередь с результатами.
    """
    logger.info(f"do_smth2: {body=}, {model_id=}")
    # Do not put result into queue
    return "Not queued result"


@app.post(params="another_method.{one}.{more}")
async def do_smth3(body: str, one: str, more: str) -> None:
    """
        Пример: функция ожидает в очереди сообщение с заголовком
                "route=another_method", обработывает body сообщения и
                заголовки "one" и "more".
    """
    logger.info(f"do_smth3: {body=}, {one=}, {more=}")
    # Function without result


if __name__ == "__main__":
    asyncio.run(app.start())
