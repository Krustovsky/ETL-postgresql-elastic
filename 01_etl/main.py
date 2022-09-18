import contextlib
import os
import datetime
from transformer import Movies

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from movies_pg import PostgresLoader

from dotenv import load_dotenv

load_dotenv()

def extract_load_transform(loader: PostgresLoader, )

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
        #for row in etl.extract_movies_upt():
            #print(row['modified'])
            #print(f'{{"index": {{"_index": "example_index", "_id": "{row["id"]}"}}}},')
            #print(f'{Movies(**row).json()},')