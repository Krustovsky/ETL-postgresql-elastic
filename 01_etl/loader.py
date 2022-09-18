import transformer
from movies_pg import PostgresLoader
import save_state
state = save_state.State(save_state.JsonFileStorage)

class Loader:
    def __init__(self, pg_loader: PostgresLoader, state: save_state.State):
        self.pg_loader = pg_loader
        self.state = state

    def update_movies(self):
        self.state.set_state('movie_time', self.pg_loader.get_db_time())
        start_time = self.state.get_state('start_time_movie')
        if not start_time:
            start_time = self.pg_loader.get_min_time()
        data = self.pg_loader.extract_movies_upt()
        return data

    #give objects
    #funvtion create index instruction with ID
    #add data to load

data_to_load = {}
#a = transformer.Movies()