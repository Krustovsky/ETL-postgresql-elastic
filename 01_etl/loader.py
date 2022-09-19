import transformer
from movies_pg import PostgresLoader
import json
import transformer
import logging
from elasticsearch import Elasticsearch, NotFoundError
import save_state
state = save_state.State(save_state.JsonFileStorage)

class Loader:
    def __init__(self, index: str, host: str = "http://127.0.0.1:9200",request_body: dict = {}):

        self.es = Elasticsearch(host)
        self.index = index
        #если индекс не создан, то создадим его
        try:
            indices_dict = self.es.indices.get(index=self.index)
        except NotFoundError:
            self.es.indices.create(index=self.index, **request_body)


    def esl_bulk_load(self, data_to_load):
        logging.info("стартуем ELS")
        if not data_to_load:
            return None
        data=[]
        for row in data_to_load:
            last_saved_item = (row['modified'])
            data.append({"index": {"_index": self.index, "_id": row["id"]}})
            data.append(transformer.Movies(**row).dict())
        if data:
            self.es.bulk(index=self.index, operations=data)
            return last_saved_item
        else:
            return None
