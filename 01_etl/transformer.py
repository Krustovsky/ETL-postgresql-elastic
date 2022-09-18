"Модуль трансформации данных"
from pydantic import BaseModel
import uuid

class Movies(BaseModel):
    id: uuid.UUID
    imdb_rating: float
    genre: list[str]
    title: str
    description: str
    director: list[str] = None
    actors_names: list[str]
    writers_names: list[str]
    actors: dict[str, str]
    writers: dict[str, str]
