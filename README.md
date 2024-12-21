# Подготовка партиционированной таблицы Rotten Tomatoes Movies для визуализации в Tableau

## Цель проекта
Целью проекта является подготовка данных о фильмах из Rotten Tomatoes для последующей визуализации в Tableau. Данные оптимизируются с использованием партиционирования, что позволяет упростить и ускорить работу с большими объёмами информации.

## Используемые технологии
- **PostgreSQL**: Для создания базы данных и реализации партиционирования.
- **DBeaver**: Для управления базой данных.
- **Tableau**: Для построения визуализаций на основе подготовленных данных.
- **Kaggle**: Источник данных.

## Источник данных
Перед загрузкой установил необходимую библиотеку:
```bash
   pip install kagglehub 
   ```

Данные были загружены из набора данных Kaggle:
```python
import kagglehub

# Download latest version
path = kagglehub.dataset_download("subhajournal/movie-rating")

print("Path to dataset files:", path) 
```

---

## Структура данных

### 1. Краткий обзор
- **Количество записей**: 16,638
- **Количество столбцов**: 17
- **Цель данных**: Подготовка для анализа и визуализации рейтингов фильмов в различных разрезах (жанры, годы, режиссеры и т. д.).

### 2. Полное описание столбцов

| Столбец              | Тип данных      | Описание                                                                 |
|-----------------------|-----------------|--------------------------------------------------------------------------|
| `movie_title`         | Текст           | Название фильма.                                                        |
| `movie_info`          | Текст           | Краткое описание фильма.                                                |
| `critics_consensus`   | Текст           | Общий консенсус критиков.                                               |
| `rating`              | Строка          | Возрастной рейтинг фильма (например, PG, R, G).                         |
| `genre`               | Текст           | Жанры фильма (может быть несколько).                                    |
| `directors`           | Текст           | Режиссеры фильма.                                                       |
| `writers`             | Текст           | Сценаристы фильма.                                                      |
| `cast`                | Текст           | Список актеров, участвующих в фильме.                                   |
| `in_theaters_date`    | Дата            | Дата выхода фильма в кинотеатрах.                                       |
| `on_streaming_date`   | Дата            | Дата релиза фильма на стриминговых платформах.                          |
| `runtime_in_minutes`  | Целое число     | Продолжительность фильма (в минутах).                                   |
| `studio_name`         | Текст           | Название киностудии, выпустившей фильм.                                 |
| `tomatometer_status`  | Строка          | Статус рейтинга критиков (Rotten, Fresh, Certified Fresh).              |
| `tomatometer_rating`  | Целое число     | Процент положительных отзывов от критиков.                              |
| `tomatometer_count`   | Целое число     | Количество отзывов критиков.                                            |
| `audience_rating`     | Число с плавающей точкой | Средний рейтинг аудитории.                                      |
| `audience_count`      | Целое число     | Количество отзывов зрителей.                                            |

### 3. Примеры данных

| movie_title                              | in_theaters_date | genre                                | tomatometer_rating | audience_rating | studio_name           |
|------------------------------------------|------------------|--------------------------------------|--------------------|-----------------|-----------------------|
| Percy Jackson & the Olympians: The Lightning Thief | 2010-02-12       | Action, Adventure, Sci-Fi, Fantasy  | 49                 | 53              | 20th Century Fox      |
| Please Give                              | 2010-04-30       | Comedy                              | 86                 | 64              | Sony Pictures Classics |
| 10                                       | 1979-10-05       | Comedy, Romance                     | 68                 | 53              | Warner Bros.          |

---

## Этапы реализации

### 1. Создание таблицы для предварительной обработки
Создал временную таблицу для загрузки данных и очистки `NULL` значений в столбце `on_streaming_date`:
```sql
CREATE TABLE rotten_tomatoes_movies (
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
Я загружал данные через простой import в DBeaver. 
Без участия DBeaver в консоли можно перейти в нужную базу данных и ввести следующую команду: 

```sql
COPY rotten_tomatoes_movies (
    movie_title, movie_info, critics_consensus, rating, genre, directors, writers, "cast",
    in_theaters_date, on_streaming_date, runtime_in_minutes, studio_name,
    tomatometer_status, tomatometer_rating, tomatometer_count, audience_rating, audience_count
)
FROM 'C:/Users/vitam/.cache/kagglehub/datasets/subhajournal/movie-rating/versions/1/dataset.csv'
DELIMITER ',' CSV HEADER;
```

### 3. Обновление NULL-значений
Заменил `NULL` в столбце `on_streaming_date` на значение по умолчанию:
```sql
UPDATE rotten_tomatoes_movies
SET on_streaming_date = '1800-01-01'
WHERE on_streaming_date IS NULL;
```
Понятно, что замена `NULL` значений на конкретную дату - не лучшая идея, вероятно проще было бы избавиться от таких данных, так как в дальнейшем нужно будет создавать отдельную партицию для этой даты, но в рамках проекта решил оставить.
### 4. Создание партиционированной таблицы
Далее создал таблицу с партиционированием по диапазону дат в столбце `on_streaming_date`:
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
Разбил данные на диапазоны дат:
```sql
CREATE TABLE rotten_tomatoes_movies_1800 PARTITION OF rotten_tomatoes_movies_partitioned
FOR VALUES FROM ('1800-01-01') TO ('1899-12-31');

CREATE TABLE rotten_tomatoes_movies_1900 PARTITION OF rotten_tomatoes_movies_partitioned
FOR VALUES FROM ('1900-01-01') TO ('1999-12-31');

CREATE TABLE rotten_tomatoes_movies_2000 PARTITION OF rotten_tomatoes_movies_partitioned
FOR VALUES FROM ('2000-01-01') TO ('2099-12-31');
```

### 6. Перенос данных
Перенёс очищенные данные из временной таблицы в партиционированную:
```sql
INSERT INTO rotten_tomatoes_movies_partitioned (
    id, movie_title, movie_info, critics_consensus, rating, genre, directors, writers, "cast",
    in_theaters_date, on_streaming_date, runtime_in_minutes, studio_name,
    tomatometer_status, tomatometer_rating, tomatometer_count, audience_rating, audience_count
)
SELECT * FROM rotten_tomatoes_movies;
```

### 7. Создание индексов
Для ускорения запросов я создал индексы:
```sql
CREATE INDEX idx_on_streaming_date ON rotten_tomatoes_movies_partitioned (on_streaming_date);
CREATE INDEX idx_genre_rating ON rotten_tomatoes_movies_partitioned (genre, tomatometer_rating);
```
По умолчанию индекс B-tree. 
### 8. Примеры запросов для анализа

#### 1. Фильтрация по дате релиза

```sql
SELECT *
FROM rotten_tomatoes_movies_partitioned
WHERE on_streaming_date BETWEEN '2010-01-01' AND '2010-12-31';
```

#### 2. Анализ жанров с высоким рейтингом критиков

```sql
SELECT genre, AVG(tomatometer_rating) as avg_rating
FROM rotten_tomatoes_movies_partitioned
WHERE tomatometer_rating > 70
GROUP BY genre
ORDER BY avg_rating DESC;
```

#### 3. Наиболее продуктивные студии

```sql
SELECT studio_name, COUNT(*) as movie_count
FROM rotten_tomatoes_movies_partitioned
GROUP BY studio_name
ORDER BY movie_count DESC
LIMIT 10;
```

#### 4. Анализ популярности жанров
Определение самых популярных жанров на основе среднего рейтинга зрителей, с учетом минимального числа отзывов для надежности данных.
```sql
SELECT 
    genre,
    COUNT(*) AS movie_count,
    AVG(audience_rating) AS avg_audience_rating,
    SUM(audience_count) AS total_audience_reviews
FROM 
    rotten_tomatoes_movies_partitioned
WHERE 
    audience_count > 1000
GROUP BY 
    genre
ORDER BY 
    avg_audience_rating DESC
LIMIT 10;
```
#### 5. Лучшие режиссеры
Поиск ТОП-10 режиссеров, создавших больше всего фильмов с рейтингом критиков выше 85%
```sql
SELECT 
    directors,
    COUNT(*) AS high_rating_movies_count
FROM 
    rotten_tomatoes_movies_partitioned
WHERE 
    tomatometer_rating > 85
GROUP BY 
    directors
ORDER BY 
    high_rating_movies_count DESC
LIMIT 10;
```
#### 6. Жанры, популярные в разные годы
Сравнение популярности жанров по среднему рейтингу зрителей в разные десятилетия.
```sql
SELECT 
    EXTRACT(YEAR FROM on_streaming_date)::INT / 10 * 10 AS decade,
    genre,
    AVG(audience_rating) AS avg_rating,
    COUNT(*) AS movie_count
FROM 
    rotten_tomatoes_movies_partitioned
GROUP BY 
    decade, genre
HAVING 
    COUNT(*) > 10
ORDER BY 
    decade, avg_rating DESC;
```

#### 7. Сравнение успешности студий
Определение студий с наибольшим количеством успешных фильмов (с рейтингом зрителей выше 70%).
```sql
SELECT 
    studio_name,
    COUNT(*) AS successful_movies_count,
    AVG(audience_rating) AS avg_success_rating
FROM 
    rotten_tomatoes_movies_partitioned
WHERE 
    audience_rating > 70
GROUP BY 
    studio_name
ORDER BY 
    successful_movies_count DESC
LIMIT 10;
```

---

### Возможности анализа

- **Дата релиза и популярность**: Сравнение оценок фильмов, выпущенных в разные годы.
- **Жанры и рейтинги**: Выявление наиболее популярных жанров среди зрителей и критиков.
- **Студии и качество**: Анализ успешности студий по количеству высокорейтинговых фильмов.

---
### Навыки, использованные в проекте
- Работа с реляционными базами данных (PostgreSQL).
- Партиционирование таблиц для оптимизации запросов.
- Очистка и подготовка данных.
- Создание индексов и базовая оптимизация.
- Подготовка данных для интеграции с инструментами визуализации (Tableau).

### Контакты
[Моё резюме на HeadHunter](https://rostov.hh.ru/resume/1ffeec20ff0ce23f9f0039ed1f48474b576633)

---