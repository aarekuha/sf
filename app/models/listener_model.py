from dataclasses import dataclass


@dataclass
class ListenerModel:
    """
        Разобранные параметры обёртки
        route: "путь" к группе методов - первое значение из params обёртки
        headers: список имён переменных, значения которых должны пробрасываться
                 аргументами в обёрнутую функцию
    """
    route: str
    headers: list[str]
