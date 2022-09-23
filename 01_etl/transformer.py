"Модуль трансформации данных"
import uuid
from typing import Optional

from pydantic import BaseModel


class Movies(BaseModel):
    """Класс используем для создания объектов из transformer, которые потом в loader передаем."""

    id: uuid.UUID
    imdb_rating: Optional[float]
    genre: list[str]
    title: str
    description: Optional[str]
    director: list[str] = []
    actors_names: list[str]
    writers_names: list[str]
    actors: list[dict[str, str]]
    writers: list[dict[str, str]]
