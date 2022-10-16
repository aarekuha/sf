class QueuedResponse():
    _message: str

    def __init__(self, message: str = "") -> None:
        self._message = message

    def __str__(self) -> str:
        return self._message
