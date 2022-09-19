"Модуль трансформации данных"
from pydantic import BaseModel
import uuid


class Movies(BaseModel):
    """Класс используем для создания объектов из transformer, которые потом в loader передаем."""

    id: uuid.UUID
    imdb_rating: float = None
    genre: list[str]
    title: str
    description: str = None
    director: list[str] = []
    actors_names: list[str]
    writers_names: list[str]
    actors: list[dict[str, str]]
    writers: list[dict[str, str]]
