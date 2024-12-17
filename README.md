
# Инструкция по созданию партиционированной таблицы Rotten Tomatoes Movies

## 1. Создание таблицы без партиционирования

Создайте таблицу без партиционирования, чтобы сначала удалить `NULL` из ключевого столбца:

```sql
CREATE TABLE rotten_tomatoes_movies_temp (
    id SERIAL,
    movie_title TEXT,
    movie_info TEXT,
    critics_consensus TEXT,
    rating VARCHAR(50),
    genre TEXT,
    directors TEXT,
    writers TEXT,
    "cast" TEXT,
    in_theaters_date DATE,
    on_streaming_date DATE,
    runtime_in_minutes INTEGER,
    studio_name TEXT,
    tomatometer_status VARCHAR(50),
    tomatometer_rating INTEGER,
    tomatometer_count INTEGER,
    audience_rating INTEGER,
    audience_count INTEGER
);
```

## 2. Наполнение таблицы данными

Если ваш файл находится в формате CSV, используйте команду `COPY` для загрузки данных:

```sql
COPY rotten_tomatoes_movies_temp (
    movie_title, movie_info, critics_consensus, rating, genre, directors, writers, "cast",
    in_theaters_date, on_streaming_date, runtime_in_minutes, studio_name,
    tomatometer_status, tomatometer_rating, tomatometer_count, audience_rating, audience_count
)
FROM '/path/to/your/dataset.csv'
DELIMITER ',' CSV HEADER;
```

## 3. Обновление NULL-значений

Замените `NULL` в столбце `on_streaming_date` на значение по умолчанию:

```sql
UPDATE rotten_tomatoes_movies_temp
SET on_streaming_date = '1800-01-01'
WHERE on_streaming_date IS NULL;
```

## 4. Создание партиционированной таблицы

Теперь создайте партиционированную таблицу:

```sql
CREATE TABLE rotten_tomatoes_movies_partitioned (
    id SERIAL,
    movie_title TEXT,
    movie_info TEXT,
    critics_consensus TEXT,
    rating VARCHAR(50),
    genre TEXT,
    directors TEXT,
    writers TEXT,
    "cast" TEXT,
    in_theaters_date DATE,
    on_streaming_date DATE,
    runtime_in_minutes INTEGER,
    studio_name TEXT,
    tomatometer_status VARCHAR(50),
    tomatometer_rating INTEGER,
    tomatometer_count INTEGER,
    audience_rating INTEGER,
    audience_count INTEGER,
    PRIMARY KEY (id, on_streaming_date) -- Объединенный PRIMARY KEY
) PARTITION BY RANGE (on_streaming_date);
```

## 5. Создание партиций

Создайте партиции для диапазонов дат:

```sql
CREATE TABLE rotten_tomatoes_movies_1800 PARTITION OF rotten_tomatoes_movies_partitioned
FOR VALUES FROM ('1800-01-01') TO ('1899-12-31');

CREATE TABLE rotten_tomatoes_movies_1900 PARTITION OF rotten_tomatoes_movies_partitioned
FOR VALUES FROM ('1900-01-01') TO ('1999-12-31');

CREATE TABLE rotten_tomatoes_movies_2000 PARTITION OF rotten_tomatoes_movies_partitioned
FOR VALUES FROM ('2000-01-01') TO ('2099-12-31');
```

## 6. Перенос данных во временную таблицу

Перенесите данные из временной таблицы в партиционированную таблицу:

```sql
INSERT INTO rotten_tomatoes_movies_partitioned (
    id, movie_title, movie_info, critics_consensus, rating, genre, directors, writers, "cast",
    in_theaters_date, on_streaming_date, runtime_in_minutes, studio_name,
    tomatometer_status, tomatometer_rating, tomatometer_count, audience_rating, audience_count
)
SELECT * FROM rotten_tomatoes_movies_temp;
```

## 7. Проверка распределения данных

Убедитесь, что данные корректно распределены по партициям:

```sql
SELECT on_streaming_date, COUNT(*)
FROM rotten_tomatoes_movies_partitioned
GROUP BY on_streaming_date
ORDER BY on_streaming_date;
```

## 8. Создание индексов

Для ускорения запросов создайте индексы:

```sql
CREATE INDEX idx_on_streaming_date ON rotten_tomatoes_movies_partitioned (on_streaming_date);
CREATE INDEX idx_genre_rating ON rotten_tomatoes_movies_partitioned (genre, tomatometer_rating);
```

## 9. Сравнение производительности

### Запрос 1: Фильтрация по `on_streaming_date`

```sql
EXPLAIN ANALYZE
SELECT *
FROM rotten_tomatoes_movies_partitioned
WHERE on_streaming_date BETWEEN '2010-01-01' AND '2010-12-31';
```

### Запрос 2: Фильтрация по `genre` и `tomatometer_rating`

```sql
EXPLAIN ANALYZE
SELECT *
FROM rotten_tomatoes_movies_partitioned
WHERE genre = 'Action' AND tomatometer_rating > 70;
```

## 10. Просмотр индексов

Для просмотра созданных индексов выполните:

```sql
SELECT
    tablename,
    indexname,
    indexdef
FROM
    pg_indexes
WHERE
    tablename = 'rotten_tomatoes_movies_partitioned';
```

## Итог

В данном руководстве описаны шаги по созданию временной таблицы, настройке партиционированной таблицы, загрузке данных и оптимизации запросов с использованием индексов. Используйте предоставленные команды для проверки производительности и управления данными. Если у вас есть вопросы, обращайтесь за помощью! 😊
