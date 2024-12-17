# Подготовка партиционированной таблицы Rotten Tomatoes Movies для визуализации в Tableau

## Цель проекта
Целью проекта является подготовка данных о фильмах из Rotten Tomatoes для последующей визуализации в Tableau. Данные оптимизируются с использованием партиционирования, что позволяет упростить и ускорить работу с большими объёмами информации.

## Используемые технологии
- **PostgreSQL**: Для создания базы данных и реализации партиционирования.
- **DBeaver**: Для управления базой данных.
- **Tableau**: Для построения визуализаций на основе подготовленных данных.
- **Kaggle**: Источник данных.

## Источник данных
Данные были загружены из набора данных Kaggle:
```python
import kagglehub

# Download latest version
path = kagglehub.dataset_download("subhajournal/movie-rating")

print("Path to dataset files:", path)
```

## Этапы реализации

### 1. Создание таблицы для предварительной обработки
Создайте временную таблицу для загрузки данных и очистки `NULL` значений в столбце `on_streaming_date`:
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

### 2. Загрузка данных
Используйте команду `COPY` для загрузки данных из CSV-файла:
```sql
COPY rotten_tomatoes_movies_temp (
    movie_title, movie_info, critics_consensus, rating, genre, directors, writers, "cast",
    in_theaters_date, on_streaming_date, runtime_in_minutes, studio_name,
    tomatometer_status, tomatometer_rating, tomatometer_count, audience_rating, audience_count
)
FROM '/path/to/your/dataset.csv'
DELIMITER ',' CSV HEADER;
```

### 3. Обновление NULL-значений
Замените `NULL` в столбце `on_streaming_date` на значение по умолчанию:
```sql
UPDATE rotten_tomatoes_movies_temp
SET on_streaming_date = '1800-01-01'
WHERE on_streaming_date IS NULL;
```

### 4. Создание партиционированной таблицы
Создайте таблицу с партиционированием по диапазону дат в столбце `on_streaming_date`:
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
    PRIMARY KEY (id, on_streaming_date)
) PARTITION BY RANGE (on_streaming_date);
```

### 5. Создание партиций
Разбейте данные на диапазоны дат:
```sql
CREATE TABLE rotten_tomatoes_movies_1800 PARTITION OF rotten_tomatoes_movies_partitioned
FOR VALUES FROM ('1800-01-01') TO ('1899-12-31');

CREATE TABLE rotten_tomatoes_movies_1900 PARTITION OF rotten_tomatoes_movies_partitioned
FOR VALUES FROM ('1900-01-01') TO ('1999-12-31');

CREATE TABLE rotten_tomatoes_movies_2000 PARTITION OF rotten_tomatoes_movies_partitioned
FOR VALUES FROM ('2000-01-01') TO ('2099-12-31');
```

### 6. Перенос данных
Перенесите очищенные данные из временной таблицы в партиционированную:
```sql
INSERT INTO rotten_tomatoes_movies_partitioned (
    id, movie_title, movie_info, critics_consensus, rating, genre, directors, writers, "cast",
    in_theaters_date, on_streaming_date, runtime_in_minutes, studio_name,
    tomatometer_status, tomatometer_rating, tomatometer_count, audience_rating, audience_count
)
SELECT * FROM rotten_tomatoes_movies_temp;
```

### 7. Создание индексов
Для ускорения запросов создайте индексы:
```sql
CREATE INDEX idx_on_streaming_date ON rotten_tomatoes_movies_partitioned (on_streaming_date);
CREATE INDEX idx_genre_rating ON rotten_tomatoes_movies_partitioned (genre, tomatometer_rating);
```

### 8. Примеры запросов для анализа
#### Фильтрация по дате
```sql
SELECT *
FROM rotten_tomatoes_movies_partitioned
WHERE on_streaming_date BETWEEN '2010-01-01' AND '2010-12-31';
```

#### Топ-10 фильмов по рейтингу
```sql
SELECT movie_title, tomatometer_rating
FROM rotten_tomatoes_movies_partitioned
WHERE genre = 'Action'
ORDER BY tomatometer_rating DESC
LIMIT 10;
```

#### Количество фильмов по годам
```sql
SELECT EXTRACT(YEAR FROM on_streaming_date) AS year, COUNT(*)
FROM rotten_tomatoes_movies_partitioned
GROUP BY year
ORDER BY year;
```

## Навыки, использованные в проекте
- Работа с реляционными базами данных (PostgreSQL).
- Партиционирование таблиц для оптимизации запросов.
- Очистка и подготовка данных.
- Создание индексов и базовая оптимизация.
- Подготовка данных для интеграции с инструментами визуализации (Tableau).

## Контакты
[Моё резюме на HeadHunter](https://rostov.hh.ru/resume/1ffeec20ff0ce23f9f0039ed1f48474b576633)

---