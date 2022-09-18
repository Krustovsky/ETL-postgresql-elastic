"""Основной модель для запроса данных из таблиц Postgres."""
import datetime
import uuid
from save_state import State

class PostgresLoader:
    def __init__(self, pg_conn, state, schema="", limit=100):
        """Создаем объект с коннектом и курсором.

        Args:
            pg_conn (psycopg2.extras.connection): коннект к Postgres
            schema (str): имя схемы из которой читаем
            limit (int): количество строк в одном запросе в Postgres
        """
        self.conn = pg_conn
        self.limit = limit
        self.state: State = state
        self.cur = pg_conn.cursor()
        if schema:
            self.schema: str = "{0}.".format(schema)
        else:
            self.schema = schema

    def extract_movies_upt(self, time: datetime.datetime, limit: int = 100):
        """Читаем фильмы по дате обновления

        :param limit:
        :return:
        """
        self.cur.execute(
            f"SELECT fw.id"
            f", fw.title"
            f", fw.description"
            f", fw.rating as imdb_rating"
            f", fw.type"
            f", fw.created"
            f", fw.modified"
            f", COALESCE (json_agg(DISTINCT jsonb_build_object('id', p.id,'name', p.full_name)) FILTER (WHERE p.id is not null and pfw.role = 'actor'), '[]') as actors"
            f", COALESCE (json_agg(DISTINCT jsonb_build_object('id', p.id,'name', p.full_name)) FILTER (WHERE p.id is not null and pfw.role = 'writer'), '[]') as writers"
            f",array_agg(DISTINCT g.name) as genre"
            f", COALESCE (array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'writer' ), ARRAY[]::text[]) as writers_names"
            f", COALESCE (array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'actor'), ARRAY[]::text[]) as actors_names"
            f",array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director' ) as director"
            f" from {self.schema}film_work fw"
            f" LEFT JOIN {self.schema}person_film_work pfw ON pfw.film_work_id = fw.id"
            f" LEFT JOIN {self.schema}person p ON p.id = pfw.person_id"
            f" LEFT JOIN {self.schema}genre_film_work gfw ON gfw.film_work_id = fw.id"
            f" LEFT JOIN {self.schema}genre g ON g.id = gfw.genre_id"
            f" WHERE fw.modified >= '{time}'" 
            f" GROUP BY fw.id"
            f" ORDER BY fw.modified"
            f" LIMIT {self.limit};"
        )
        if self.cur.rowcount <1:
            return None
        else:
            for row in self.cur.fetchall():
                yield dict(row)


    # Забираем обновление по жанрам
    def extract_id_genre_upd(self, time_start: datetime.datetime, time_finish: datetime.datetime, id_offset: uuid.UUID):
        if id_offset:
            id_offset = f"and fw.id > {id_offset}"
        else:
            id_offset = ""
        self.cur.execute(
            f"select id"
            f", modified from content.genre"
            f" where modified > '{time_start}'"
            f" adn modified < '{time_finish}'"
            f" {id_offset}"
            f" order by id"
            f" limit {self.limit}"
        )

        if self.cur.rowcount <1:
            return None
        else:
            results = []
            for row in self.cur.fetchall():
                results.append(row['id'])
            self.state.set_state('genre_fix_id', results[-1]) #сохранили id, когда вытащим и сохраним вся связанные фильмы, заберем пачку genre_id после этого ID
            return results

        def get_movies_id_by_genre(self, genre_ids: list, time_offset: tuple) -> list:
            if not genre_ids:
                return None
            genre_ids = "'" + '\', \''.join(str(_) for _ in genre_ids) + "'"

            if time_offset:
                time_offset = f"fw.modified >= {time_offset[1]}" \
                            f" and fw.id != {time_offset[0]}"

            self.cur.execute(
                f"select fw.id"
                f", fw.modified"
                f" from content.genre g"
                f" LEFT JOIN content.genre_film_work gfw"
                f" ON g.id = gfw.genre_id"
                f" LEFT JOIN content.film_work fw"
                f" ON gfw.film_work_id = fw.id"
                f" where g.id in ({genre_ids})"
                f" group by fw.id, fw.modified"
                f" having {time_offset}"
                f" order by fw.modified"
                f" limit {self.limit}"
            )

            if self.cur.rowcount < 1:
                return None
            else:
                results = []
                for row in self.cur.fetchall():
                    results.append(row['id'])
                return results


    def update_movies_by_id(self, id_list):
        id_list = "'" + '\', \''.join(str(_) for _ in id_list) + "'"
        self.cur.execute(
            f"SELECT fw.id"
            f", fw.title"
            f", fw.description"
            f", fw.rating as imdb_rating"
            f", fw.type"
            f", fw.created"
            f", fw.modified"
            f", COALESCE (json_agg(DISTINCT jsonb_build_object('id', p.id,'name', p.full_name)) FILTER (WHERE p.id is not null and pfw.role = 'actor'), '[]') as actors"
            f", COALESCE (json_agg(DISTINCT jsonb_build_object('id', p.id,'name', p.full_name)) FILTER (WHERE p.id is not null and pfw.role = 'writer'), '[]') as writers"
            f",array_agg(DISTINCT g.name) as genre"
            f", COALESCE (array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'writer' ), ARRAY[]::text[]) as writers_names"
            f", COALESCE (array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'actor'), ARRAY[]::text[]) as actors_names"
            f",array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director' ) as director"
            f" from {self.schema}film_work fw"
            f" LEFT JOIN {self.schema}person_film_work pfw ON pfw.film_work_id = fw.id"
            f" LEFT JOIN {self.schema}person p ON p.id = pfw.person_id"
            f" LEFT JOIN {self.schema}genre_film_work gfw ON gfw.film_work_id = fw.id"
            f" LEFT JOIN {self.schema}genre g ON g.id = gfw.genre_id"
            f" WHERE fw.id in ({id_list}))"
            f" GROUP BY fw.id"
            f" ORDER BY fw.modified"
            f" LIMIT {self.limit};"
        )

    def get_db_time(self):
        self.cur.execute(
            f'select now() as current_time'
        )
        return self.cur.fetchall()[0]['current_time']

    def get_min_modified_time(self):
        self.cur.execute(
            f'select min(modified) as min_time from content.film_work'
        )
        return self.cur.fetchall()[0]['min_time']