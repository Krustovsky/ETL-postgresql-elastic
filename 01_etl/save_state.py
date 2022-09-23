"""Модуль хранния состояний процесса ETL"""
import abc
import json
from typing import Any, Optional


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    """Реализует методы класса BaseStorage для хранения состояний в JSON"""

    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path if file_path else "state.json"


    def save_state(self, state: dict) -> None:
        """Сохранить словарь состояний в файл"""
        with open(self.file_path, "w") as f:
            json.dump(state, f)

    def retrieve_state(self) -> dict:
        """Прочитать словарь состояний из файла"""
        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            return {}


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""

        state = self.storage.retrieve_state()
        state.update({key: value})
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.storage.retrieve_state().get(key)
