# Заключительное задание первого модуля

# Алгоритм загрузки.
У нас есть функция main, которая создает необходимые объекты с коннекторами к Postgres и Elasticsearch, далее есть функция:
- `extract_load_transform` сначала грузит данные из таблицы `film_works`, а потом вызывает функцию:
- `etl_linked_tables` для таблиц genre и person чтобы загрузить фильмы связанные с обновленными жанрами и людьми.
Вся загрузка происходит через объект класса `PostgresLoader`.
- мы везде данные сортируем по `modified` и грузим количество строк по limit, который указываем при создании объекта класса `PostgresLoader`.
- чтобы загрузить следующую пачку данных мы смещаемся на `modified` последнего элемента, который считали в прошлый раз.
- при этом смещаемся мы через сохраненные и чтение состояния в State, сохранение происходит только после загрузки данных в Elasticsearch.
Если процесс падает все состояния сохранены и при старте подгружаются.
Можно добавить состояние чтобы при падении мы начинали именно с загрузки по жанрам или по людям, если упали на них, но не увидел смысла по бизнесу, например упали мы на час, за это время нагрузили фильмов или поизменяли их, мы должны как раз в первую очередь подгрузить данные из таблицы с фильмами, а потом уже тащить продолжить тащить обновление по людям и жанрам. Сам фильм важнее чем исправленная фамилия, а тем более жанр.

Только перед первоначальной загрузкой, мы сохраняем время равное текущиму в БД, с которого стоит стартовать обновления по людям и фильмам, так как мы загрузим полностью таблицу с фильмами, и все изменения по людям и жанрам до старта загрузки будут учтены, соответственно, мы не будем повторно грузить фильмы, которые пришлось бы перегонять в Elastic если стартовать обновление по людям и жанрам с начала.

Все данные грузятся в ElasticSearch через класс `Loader`. Там все просто, данные трансформируются через `pydantic` класс `Movie`.

## Упрощенно алгоритм такой:
- Выгружаем обновленные фильмы пачками сортируя по дате изменяния и грузим пачками в Elasticsearch.
- Далее выгружаем по обновлению элементов связанных таблиц M2M с людьми и жанрами.
- Читаем пачку людей/жанров (идем тажке по дате обновления)
- Выгружаем связанные с ними фильмы пачками сортируя по дате изменяния (`modified`) и грузим пачками в Elasticsearch.
- Потом читаем новую пачку людей/жанров и т.д. пока они не кончатся.
- Как только пройдемся по обновленным людям и жанрам, начинаем все сначала.

elasticsearch использовался версии 8.4.
Docker-compose в проекте.
Важно отключить xpack.security.enabled=false чтобы не заморачиваться с безопасностью при подключении, так как там уже конкретный способ нужно выбирать для прода.

Функция `main()` обернута декоратором `backoff`, чтобы если Postgres или Elastic упадут, продолжить попытки работы.
