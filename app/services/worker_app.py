import asyncio
import inspect
from typing import Any
from typing import Callable
from asyncio import AbstractEventLoop
from aio_pika.abc import AbstractIncomingMessage
from aio_pika.abc import HeadersType

from models import ListenerModel
from models import QueuedResponse
from .queue import Queue
from .service_base import ServiceBase


class WorkerApp(ServiceBase):
    """ Фреймворк для работы с RabbitMQ """
    DELIMITER = "."  # Разделитель имён переменных в параметрах обёрток

    # Имя заголовка с содержанием "пути" (например, app.post("ПУТЬ.{ИМЯ_ПЕРЕМЕННОЙ}")
    METHOD_KEY = "route"

    _queue: Queue
    _listeners: dict[Callable, ListenerModel]

    def __init__(self, queue: Queue) -> None:
        super().__init__()
        self._queue: Queue = queue
        self._listeners = {}

    def _parse_params(self, params: str) -> ListenerModel:
        """
            Преобразование параметров обёртки в переменные
                для отбора консьюмером из заголовков (см. ListenerModel)
        """
        splitted_params: list[str] = params.split(self.DELIMITER)
        route: str = splitted_params[0]
        headers: list[str] = [v.strip("{").strip("}") for v in splitted_params[1:]]
        return ListenerModel(
            route=route,
            headers=headers,
        )

    def _make_kwargs(
        self,
        func_name: str,
        kwargs: dict[str, Any],
        func_args: list[str]
    ) -> dict[str, Any]:
        """
            Подготовка аргументов для использования в обёрнутой функции
            - исключение в случае, когда в аргументах обёрнутой функции
              присутствует запрос обязательного, а в данных из "исходной"
              очереди такой аргумент отсутствует
            - подбор только тех аргументов, которые есть в запрошенных
              обёрнутой функцией

            !!! Аргумент body в оборачиваемой функции должен присутствовать всегда

        """
        result_kwargs: dict[str, Any] = {}
        for func_arg in func_args:
            if func_arg not in kwargs:
                raise Exception(
                    f"Message not contains required arg: {func_name=}, {func_arg=}, {kwargs=}"
                )

        for key, value in kwargs.items():
            if key in func_args:
                result_kwargs[key] = value

        return result_kwargs

    def post(self, params: str) -> Callable:
        """
            Обёртка для функций, которые будут вызываться, при появлении сообщения в очереди
            Правила описания параметров(params):
                - первым аргументом необходимо указать путь(route)
                    например, (params="ПУТЬ"), (params="train_model")
                - затем, после route можно указать через точку имена переменных(заголовков),
                  которые должны присутствовать в сообщении с указанным путём
                    например, (params="ПУТЬ.{ИМЯ_ПУРЕМЕННОЙ}.{ИМЯ_ДРУГОЙ_ПЕРЕМЕННОЙ}")
                              (params="train_model.{model_id}")

            !!! Аргумент body в оборачиваемой функции должен присутствовать всегда

            P.S. имена переменных можно заключать в фигурные скобки, они убираются во время
                 парсинга параметров (self._parse_params)
        """
        def outer_wrapper(func):
            async def inner_wrapper(*args, **kwargs):
                # Забор через инспектор параметров обёрнутой функции
                func_args: list[str] = list(inspect.signature(func).parameters)
                func_kwargs: dict[str, Any] = self._make_kwargs(
                    func_name=func.__name__,
                    kwargs=kwargs,
                    func_args=func_args,
                )
                # Вызов функции для выполнения обработки
                result: Any = await func(*args, **func_kwargs)
                # Для отправки результатов в очередь с обработанными, они должны
                #     соответствовать определенной модели (QueuedResponse)
                if result and isinstance(result, QueuedResponse):
                    # Отправка результатов обработки в соответствующую очередь
                    #    если была передана её конфигурация к ней было установлено
                    #    подключение
                    await self._queue.put_result(str(result))
            # Добавление функции в список обрабатываемых, при получении
            #     сообщения в исходной очереди
            self._listeners[inner_wrapper] = self._parse_params(params)
            return inner_wrapper

        return outer_wrapper

    def _get_listner_by_route(self, route: str):
        """
            Выбор подписанных на указанный route обёрнутых функций
            route(путь): первый аргумент в params обёртки функций
                   например, (params="ПУТЬ.{ИМЯ_ПЕРЕМЕННОЙ}")
        """
        return {func for func, listener in self._listeners.items() if listener.route == route}

    def _get_message_kwargs(self, headers: HeadersType) -> dict[str, str]:
        """ Разбор заголовков сообщения из очереди в словарь """
        kwargs: dict[str, str] = {k: str(v) for k, v in headers.items() if k != self.METHOD_KEY}
        return kwargs

    async def _process(self, message: AbstractIncomingMessage) -> None:
        """ Обработка отдельного сообщения """
        async with message.process():
            if self.METHOD_KEY in message.headers:
                route: str = str(message.headers[self.METHOD_KEY])
                for func in self._get_listner_by_route(route):
                    kwargs: dict[str, str] = self._get_message_kwargs(headers=message.headers)
                    kwargs["body"] = message.body.decode()
                    await func(**kwargs)

    async def start(self) -> None:
        """ Запуск сервиса """
        if not self._listeners:
            raise Exception(f"Has no listeners...")

        loop: AbstractEventLoop = asyncio.get_event_loop()
        await self._queue.consume(loop=loop, callback=self._process)
