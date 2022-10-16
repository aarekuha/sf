import logging


class ServiceBase:
    logger: logging.Logger

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f"Initiated...")
