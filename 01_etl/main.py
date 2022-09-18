import contextlib
import os
import datetime
from loader import Loader
from transformer import Movies

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from movies_pg import PostgresLoader

from dotenv import load_dotenv

load_dotenv()

def extract_load_transform(loader: PostgresLoader):
    def update_movies(self):
        self.state.set_state('movie_time', self.pg_loader.get_db_time())
        start_time = self.state.get_state('start_time_movie')
        if not start_time:
            start_time = self.pg_loader.get_min_time()
        data = self.pg_loader.extract_movies_upt()
        return data
from dotenv import load_dotenv
if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('DB_NAME'),
           'user': os.environ.get('DB_USER'),
           'password': os.environ.get('DB_PASSWORD'),
           'host': os.environ.get('DB_HOST'),
           'port': os.environ.get('DB_PORT')}

    with contextlib.closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
        etl = PostgresLoader(pg_conn, schema="content")
        print(etl.get_db_time())
        print(datetime.datetime.now())
        es = Loader("example_index")
        print(es.esl_bulk_load(etl.extract_movies_upt(time=datetime.datetime(2020, 6, 16, 20, 14, 9, 232139, tzinfo=datetime.timezone.utc))))
        #for row in etl.extract_movies_upt():
            #print(row['modified'])
            #print(f'{{"index": {{"_index": "example_index", "_id": "{row["id"]}"}}}},')
            #print(f'{Movies(**row).json()},')
        print(etl.get_movies_id_genre())