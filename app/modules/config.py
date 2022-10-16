import os
import dotenv
import logging


class Config():
    LOG_FORMAT = "%(asctime)s [%(name)s:%(lineno)s] [%(levelname)s]: %(message)s"

    SRC_QUEUE_NAME = "hello"
    SRC_QUEUE_HOST = "localhost"
    SRC_QUEUE_PORT = "5672"
    SRC_QUEUE_USERNAME = "guest"
    SRC_QUEUE_PASSWORD = "guest"

    DST_QUEUE_NAME = "results"
    DST_QUEUE_HOST = "localhost"
    DST_QUEUE_PORT = "5672"
    DST_QUEUE_USERNAME = "guest"
    DST_QUEUE_PASSWORD = "guest"

    DEBUG = 0  # Детализация уровня логирования (DEBUG=0 - продуктовый режим)

    def __init__(self):
        """
            Данные сливаются в конфигурацию (класс) из системного окружения
            * использовать файл .env или docker-environment
        """
        dotenv.load_dotenv()
        self.__dict__.update(os.environ)

        logging.basicConfig(level=self.log_level, format=self.LOG_FORMAT)


    @property
    def log_level(self):
        """ Уровень логирования, в зависимости от режима DEBUG """
        return "DEBUG" if self.DEBUG and str(self.DEBUG) != "0" else "INFO"
