import asyncio
import aio_pika
from typing import Callable
from asyncio import AbstractEventLoop
from aio_pika import Message
from aio_pika.abc import AbstractQueue
from aio_pika.abc import AbstractRobustConnection
from aio_pika.abc import AbstractChannel
from aio_pika.abc import AbstractExchange

from .service_base import ServiceBase


class Queue(ServiceBase):
    """
        Сервис для работы с очередью
        Обязательные аргументы при инициализации:
            src_queue_name: имя исходной очереди
            src_queue_host: хост RabbitMQ
            src_queue_username: пользователь RabbitMQ
            src_queue_password: пароль пользователя RabbitMQ

        Необязательные аргументы:
            src_queue_port: порт RabbitMQ

            dst_queue_name: имя очереди с результатами обработки(целевая)
            dst_queue_host: хост целевого RabbitMQ
            dst_queue_port: порт цеоевого RabbitMQ
            dst_queue_username: пользователь целевого RabbitMQ
            dst_queue_password: пароль пользователя целевого RabbitMQ

        !!! При указании имени очереди для результатов обработки необходимо
            указать следующие аргументы:
                - dst_queue_host
                - dst_queue_username
                - dst_queue_password

        Порт для обоих очередей указывать не обязательно, в этом случае он не
           добавляется в dsn и подключение происходит к порту
           "по умолчанию" - 5672
    """
    DEFAULT_PREFETCH_COUNT = 100
    RECONNECT_DELAY_SEC = 1  # Секунд до повторного подключения к очереди с результатами

    _loop: AbstractEventLoop
    _src_queue_name: str
    _dsn_src_queue: str

    _dst_queue_name: str | None = None
    _dsn_results_queue: str | None = None
    _results_exchange: AbstractExchange | None = None
    _dst_queue_connection: AbstractRobustConnection | None = None

    def __init__(
        self,
        src_queue_name: str,
        src_queue_host: str,
        src_queue_username: str,
        src_queue_password: str,
        src_queue_port: str = "",
        dst_queue_name: str = "",
        dst_queue_host: str = "",
        dst_queue_port: str = "",
        dst_queue_username: str = "",
        dst_queue_password: str = "",
    ) -> None:
        """ Описание аргументов см. в docstring класса """
        super().__init__()
        # Подготовка настроек для подключения к "исходной" очереди
        self._src_queue_name = src_queue_name
        maked_src_queue_port: str = f":{src_queue_port}" if src_queue_port else ""
        self._dsn_src_queue = f"amqp://{src_queue_username}:{src_queue_password}" \
                                  f"@{src_queue_host}{maked_src_queue_port}/"
        if dst_queue_name:
            # Проверка наличия всех необходимых параметров
            if not all(
                [
                    dst_queue_host,
                    dst_queue_username,
                    dst_queue_password,
                ]
            ):
                raise Exception(
                    "Declared destination queue name, but not enough parameters. "
                    "Please, fill all parameters [host, username and password] "
                    "or refuse destination queue"
                )
            self._dst_queue_name = dst_queue_name
            maked_dst_queue_port: str = f":{dst_queue_port}" if dst_queue_port else ""
            self._dsn_results_queue = f"amqp://{dst_queue_username}:{dst_queue_password}" \
                                      f"@{dst_queue_host}{maked_dst_queue_port}/"

    async def _connect_to_results_queue(self) -> None:
        """ Установка подключения к очереди с результатами обработки """
        if self._dsn_results_queue:
            connection: AbstractRobustConnection = await aio_pika.connect_robust(self._dsn_results_queue)
            channel: AbstractChannel = await connection.channel()
            await channel.set_qos(prefetch_count=self.DEFAULT_PREFETCH_COUNT)
            self._results_exchange = channel.default_exchange
            self._dst_queue_connection = connection
            await channel.declare_queue(self._dst_queue_name, auto_delete=False)

    async def consume(self, loop: AbstractEventLoop, callback: Callable) -> None:
        """ Запуск консьюмера из "исходной" очереди """
        self._loop: AbstractEventLoop = asyncio.get_event_loop() if not loop else loop
        try:
            connection: AbstractRobustConnection = await aio_pika.connect_robust(self._dsn_src_queue)
        except ConnectionError:
            self.logger.error("Connection error. Reconnecting...")
            await asyncio.sleep(self.RECONNECT_DELAY_SEC)
            await self.consume(loop=loop, callback=callback)
            return

        self.logger.info("Queue connected...")
        channel: AbstractChannel = await connection.channel()
        await channel.set_qos(prefetch_count=self.DEFAULT_PREFETCH_COUNT)
        queue: AbstractQueue = await channel.declare_queue(
            self._src_queue_name,
            auto_delete=False,
        )
        # "Подключение" метода обработки полученных сообщений
        await queue.consume(callback)
        self.logger.info("Listener started...")
        # Ожидание завершения обработки очереди
        try:
            await asyncio.Future()
        finally:
            await connection.close()
            if self._dst_queue_connection:
                await self._dst_queue_connection.close()

    async def put_result(self, message: str) -> None:
        """ Отправка данных в очередь с результатами обработки """
        if not self._dst_queue_name:
            return

        if not self._results_exchange:
            # Установка нового соединения с "целевой" очередью
            await self._connect_to_results_queue()

        if self._results_exchange:
            try:
                await self._results_exchange.publish(
                    message=Message(message.encode()),
                    routing_key=self._dst_queue_name,
                )
            except Exception as exception:
                self.logger.exception(exception)
                # Сброс текущего подключения, после повторного вызова соединение будет устанавливаться заново
                # TODO: найти класс исключения для обрыва и обрабатывать таким образом только его
                self._results_exchange = None
                await asyncio.sleep(self.RECONNECT_DELAY_SEC)
                # Переподключение и повторная попытка отправки сообщения
                await self.put_result(message=message)
