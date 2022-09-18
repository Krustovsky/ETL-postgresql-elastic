"""Основной модель для запроса данных из таблиц Postgres."""


class PostgresLoader:
    def __init__(self, pg_conn, schema="", limit=100):
        """Создаем объект с коннектом и курсором.

        Args:
            pg_conn (psycopg2.extras.connection): коннект к Postgres
            schema (str): имя схемы из которой читаем
            limit (int): количество строк в одном запросе в Postgres
        """
        self.conn = pg_conn
        self.cur = pg_conn.cursor()
        if schema:
            self.schema: str = "{0}.".format(schema)
        else:
            self.schema = schema

    def extract_movies_upt(self, limit: int = 100):
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
            f" WHERE fw.modified >= '2020-06-16 20:14:09.222069+00:00'" 
            f" GROUP BY fw.id"
            f" ORDER BY fw.modified"
            f" LIMIT {limit};"
        )

        for row in self.cur.fetchall():
            yield dict(row)


    def extract_id_genre_upd(self, limit: int = 100):
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
            f" WHERE fw.modified >= '2020-06-16 20:14:09.222069+00:00'" 
            f" GROUP BY fw.id"
            f" ORDER BY fw.modified"
            f" LIMIT {limit};"
        )

    def get_db_time(self):
        self.cur.execute(
            f'select now() as current_time'
        )
        return self.cur.fetchall()[0]['current_time']

    def get_min_time(self):
        self.cur.execute(
            f'select min(modified) as min_time from content.film_work'
        )
        return self.cur.fetchall()[0]['min_time']