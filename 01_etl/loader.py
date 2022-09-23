"""Модуль загрузки данных в elasticsearch"""
import datetime
import logging
from typing import Optional

import transformer
from elasticsearch import Elasticsearch, NotFoundError


class Loader:
    """Класс загузчика данных в elasticsearch"""

    def __init__(self, es: Elasticsearch, index: str, request_body: dict = {}):
        self.es = es
        self.index = index
        try:
            indices_dict = self.es.indices.get(index=self.index)
        except NotFoundError:
            self.es.indices.create(index=self.index, **request_body)

    def esl_bulk_load(self, data_to_load) -> Optional[datetime.datetime]:
        """Функций bulk загрузки данных в ETL.

        :param data_to_load: генератор данных для загрузки
        :return: datetime.datetime - значение modified последнего загруженного элемента
        """
        logging.debug("Starting ELS")
        if not data_to_load:
            return None
        data = []
        for row in data_to_load:
            last_saved_item = row["modified"]
            data.append({"index": {"_index": self.index, "_id": row["id"]}})
            data.append(transformer.Movies(**row).dict())
        if data:
            resp = self.es.bulk(index=self.index, operations=data)
            if resp['errors']:
                raise Exception("Ошибка загрузки данных в Elasticsearch")
            return last_saved_item
        else:
            return None
