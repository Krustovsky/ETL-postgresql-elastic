"""Основной модель для запроса данных из таблиц Postgres."""
import datetime
import uuid
class PostgresLoader:
    def __init__(self, pg_conn, schema="", limit=100):
        """Создаем объект с коннектом и курсором.

        Args:
            pg_conn (psycopg2.extras.connection): коннект к Postgres
            schema (str): имя схемы из которой читаем
            limit (int): количество строк в одном запросе в Postgres
        """
        self.conn = pg_conn
        self.limit = limit
        self.cur = pg_conn.cursor()
        if schema:
            self.schema: str = "{0}.".format(schema)
        else:
            self.schema = schema

    def extract_movies_upt(self, time_offset: datetime.datetime):
        """Читаем фильмы по дате обновления

        :param limit:
        :return: фильмы (генератор)
        """
        if time_offset:
            time_offset = f"where fw.modified > '{time_offset}'"
        else:
            time_offset=""

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
            f", COALESCE (array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'director'), ARRAY[]::text[]) as director"
            f" from {self.schema}film_work fw"
            f" LEFT JOIN {self.schema}person_film_work pfw ON pfw.film_work_id = fw.id"
            f" LEFT JOIN {self.schema}person p ON p.id = pfw.person_id"
            f" LEFT JOIN {self.schema}genre_film_work gfw ON gfw.film_work_id = fw.id"
            f" LEFT JOIN {self.schema}genre g ON g.id = gfw.genre_id"
            f" {time_offset}" 
            f" GROUP BY fw.id"
            f" ORDER BY fw.modified"
            f" LIMIT {self.limit};"
        )
        if self.cur.rowcount <1:
            return None
        else:
            for row in self.cur.fetchall():
                yield dict(row)

    def ext_id_from_linked_table(self, time_start: datetime.datetime, table: str):
        """Забираем обновление по из смежных таблиц жанры или люди

        :param time_start:
        :param table:
        :return:
        """
        if table not in ['person', 'genre']:
            raise Exception('Не указана теблица или такой таблицы нет в списке: genre, person')
        self.cur.execute(
            f"select id"
            f", modified from {self.schema}{table}"
            f" where modified > '{time_start}'"
            f" order by modified"
            f" limit {self.limit}"
        )

        if self.cur.rowcount <1:
            return None
        else:
            results = []
            time_offset = None
            for row in self.cur.fetchall():
                results.append(row['id'])
                time_offset = row['modified']
            return results, time_offset

    def get_movies_id_by_linker_id(self, ids: list, time_offset: datetime.datetime, table: str) -> list:
        """Забираем лист ID фильмов линкованых к ID таблицы table

        :param ids:
        :param time_offset:
        :param table:
        :return:
        """

        if not ids:
            return None
        if table not in ['person', 'genre']:
            raise Exception('Не указана теблица или такой таблицы нет в списке: genre, person')

        ids = "'" + '\', \''.join(str(_) for _ in ids) + "'"

        if time_offset:
            time_offset = f"having fw.modified > '{time_offset}'"
        else:
            time_offset=""

        self.cur.execute(
            f"select fw.id"
            f", fw.modified"
            f" from {self.schema}{table} l"
            f" LEFT JOIN {self.schema}{table}_film_work lfw"
            f" ON l.id = lfw.{table}_id"
            f" LEFT JOIN {self.schema}film_work fw"
            f" ON lfw.film_work_id = fw.id"
            f" where l.id in ({ids})"
            f" group by fw.id, fw.modified"
            f" {time_offset}"
            f" order by fw.modified"
            f" limit {self.limit}"
        )

        if self.cur.rowcount <1:
            return None
        else:
            results = []
            time_offset = None
            for row in self.cur.fetchall():
                results.append(row['id'])
            return results


    def update_movies_by_id(self, id_list):
        """Загрузка фильмов по ID.

        :param id_list:
        :return:
        """
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
            f", array_agg(DISTINCT g.name) as genre"
            f", COALESCE (array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'writer' ), ARRAY[]::text[]) as writers_names"
            f", COALESCE (array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'actor'), ARRAY[]::text[]) as actors_names"
            f", COALESCE (array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'director'), ARRAY[]::text[]) as director"
            f" from {self.schema}film_work fw"
            f" LEFT JOIN {self.schema}person_film_work pfw ON pfw.film_work_id = fw.id"
            f" LEFT JOIN {self.schema}person p ON p.id = pfw.person_id"
            f" LEFT JOIN {self.schema}genre_film_work gfw ON gfw.film_work_id = fw.id"
            f" LEFT JOIN {self.schema}genre g ON g.id = gfw.genre_id"
            f" WHERE fw.id in ({id_list})"
            f" GROUP BY fw.id"
            f" ORDER BY fw.modified"
            f" LIMIT {self.limit};"
        )

        if self.cur.rowcount <1:
            return None
        else:
            for row in self.cur.fetchall():
                yield dict(row)


    def get_db_time(self):
        self.cur.execute(
            f'select now() as current_time'
        )
        return self.cur.fetchall()[0]['current_time']