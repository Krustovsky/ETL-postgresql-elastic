import contextlib
import json
import logging
import os
from pathlib import Path

import psycopg2
from backoff import backoff
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from extract_pg import PostgresLoader
from loader import Loader
from psycopg2.extras import DictCursor
from save_state import JsonFileStorage, State

# загрузим схему ElasticSearch
BASE_DIR = Path(__file__).resolve().parent
with open(BASE_DIR / "ELS_scheme.json", "r") as f:
    request_body = json.load(f)


@backoff()
def main():
    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
    }
    state = State(JsonFileStorage())
    with contextlib.closing(
        psycopg2.connect(**dsl, cursor_factory=DictCursor)
    ) as pg_conn:
        extract = PostgresLoader(pg_conn, schema="content")
        es = Elasticsearch(os.environ.get("ELS_HOST"))
        loader = Loader(es, index="movies", request_body=request_body)
        extract_load_transform(extract, loader, state)


def etl_linked_tables(
    extract: PostgresLoader, loader: Loader, state: State, table: str
) -> None:
    while True:
        logging.debug("Selecting ids from linked table")

        id_list = extract.ext_id_from_linked_table(
            state.get_state(f"{table}_start_time"), table=table
        )

        logging.debug(f"list_id {id_list}")

        if not id_list:
            logging.debug(
                f"Selecting from {table} is over, no more ids"
            )
            break
        linked_table_time_offset = id_list[1]

        state.set_state(
            "internal_film_time_offset", None
        )
        logging.debug(
            f'Set internal_film_time_offset {state.get_state("internal_film_time_offset")}'
        )

        while True:
            time_offset = state.get_state("internal_film_time_offset")

            logging.debug(f"Loading movies linked with {table} ids")

            movie_id_list = extract.get_movies_id_by_linker_id(
                id_list[0], time_offset, table=table
            )
            if movie_id_list:
                resp = loader.esl_bulk_load(extract.extract_movies_upt(id_list=movie_id_list))
                state.set_state("internal_film_time_offset", str(resp))
                logging.debug(
                    f'Set internal_film_time_offset {state.get_state("internal_film_time_offset")}'
                )

            else:
                state.set_state(f"{table}_start_time", str(linked_table_time_offset))
                logging.debug(f"Loaded all movies for {table} ids pack")
                break


def extract_load_transform(
    extract: PostgresLoader, loader: Loader, state: State
) -> None:
    logging.debug("New cycle start")

    resp = True
    logging.debug("Starting selecting updates for film_work.modified")
    while resp:
        logging.debug("Moving movies from film_work.modified")
        time_offset = state.get_state("time_offset")
        logging.debug(f"Starting from {time_offset}")
        if (
            not time_offset
        ):  # если это самый первый запуск, то свдинем запуск обновления по жанрам и людям на время старта загрузки по фильмам, чтоб их не грузить повторно
            link_table_time = extract.get_db_time()
            state.set_state("genre_start_time", str(link_table_time))
            state.set_state("person_start_time", str(link_table_time))

        resp = loader.esl_bulk_load(extract.extract_movies_upt(time_offset))
        logging.debug(f"Response from elastic after loading {resp}")
        if resp:
            time_offset = resp
            state.set_state("time_offset", str(time_offset))
            logging.debug(state.get_state("time_offset"))

    logging.debug("Starting loading updates for genre.modified")
    etl_linked_tables(extract, loader, state, table="genre")

    logging.debug("Starting loading updates for person.modified")
    etl_linked_tables(extract, loader, state, table="person")


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(
        filename="etl.log",
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
    )
    while True:
        main()
