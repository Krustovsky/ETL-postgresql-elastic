import contextlib
import os
import time
import datetime
from loader import Loader
from transformer import Movies
from save_state import State, JsonFileStorage
import json
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from movies_pg import PostgresLoader
from pathlib import Path
import logging
from backoff import backoff

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
with open(BASE_DIR / 'ELS_scheme.json', 'r') as f:
    request_body = json.load(f)

@backoff()
def main():
    dsl = {'dbname': os.environ.get('DB_NAME'),
           'user': os.environ.get('DB_USER'),
           'password': os.environ.get('DB_PASSWORD'),
           'host': os.environ.get('DB_HOST'),
           'port': os.environ.get('DB_PORT')}
    state = State(JsonFileStorage())
    with contextlib.closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
        extract = PostgresLoader(pg_conn, schema="content")
        loader = Loader("example_index", request_body=request_body)
        extract_load_transform(extract, loader, state)


def etl_linked_tables(extract: PostgresLoader, loader: Loader, state: State, table: str) -> None:
    while True:
        logging.info('Выбираем linked id')
        id_list = extract.ext_id_from_linked_table(state.get_state(f'{table}_start_time'), table=table)
        logging.info(f"list_id {id_list}")
        if not id_list:
            logging.info(f'Обновлений по смежной таблице {table} больше нет, выходим из цикла')
            break
        linked_table_time_offset = id_list[1]

        state.set_state('internal_film_time_offset', None)  # используем чтобы внутри пробегаться по id фильмов
        logging.info(f'Установили internal_film_time_offset {state.get_state("internal_film_time_offset")}')

        while True:
            time_offset = state.get_state('internal_film_time_offset')

            logging.info(f'грузим фильмы связанные с обновленными {table} id')

            movie_id_list = extract.get_movies_id_by_linker_id(id_list[0], time_offset, table=table)
            if movie_id_list:
                resp = loader.esl_bulk_load(extract.update_movies_by_id(movie_id_list))
                state.set_state('internal_film_time_offset', str(resp))
                logging.info(
                    f'Установили internal_film_time_offset {state.get_state("internal_film_time_offset")}')

            else:
                state.set_state(f'{table}_start_time', str(linked_table_time_offset))
                logging.info(f'для пачки {table} id забрали все фильмы')
                break
def extract_load_transform(extract: PostgresLoader, loader: Loader, state: State) -> None:
    while True:
        logging.info('Старт основного цикла')
        resp = True
        logging.info('Стартуем сбор обновлений по film_work.modified')
        while resp:
            logging.info('Копируем фильмы по film_work.modified')
            time_offset = state.get_state('time_offset')
            logging.info(f'Стартуем с {time_offset}')
            if not time_offset:  # если это самый первый запуск, то свдинем запуск обновления по жанрам и людям на время старта загрузки по фильмам, чтоб их не дуюлировать
                link_table_time = extract.get_db_time()
                state.set_state('genre_start_time', str(link_table_time))
                state.set_state('person_start_time', str(link_table_time))
            resp = loader.esl_bulk_load(extract.extract_movies_upt(time_offset))
            logging.info(f"Ответ после загрузки {resp}")
            time.sleep(3)
            if resp:
                time_offset = resp
                state.set_state('time_offset', str(time_offset))
                logging.info(state.get_state('time_offset'))

        logging.info('Стартуем сбор обновлений по genre.modified')
        time.sleep(3)
        etl_linked_tables(extract, loader, state, table='genre')

        logging.info('Стартуем сбор обновлений по person.modified')
        time.sleep(3)
        etl_linked_tables(extract, loader, state, table='person')

if __name__ == '__main__':
    logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    main()

