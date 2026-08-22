# ETL-pipeline

Учебный проект для практики работы с **Apache Airflow, PostgreSQL, Docker, MinIO/S3 и Python**.

## Стек

* Python
* Apache Airflow
* PostgreSQL
* Docker
* Docker Compose
* MinIO / S3
* SQL

## Структура проекта

```text
PRACTICE_1/
├── config/                 # Конфигурационные файлы
├── csv файлы/              # CSV-файлы для работы
├── dags/                   # DAG-файлы Airflow
│   └── practice/
│       ├── .env            # Переменные окружения
│       └── practice_etl.py # ETL-пайплайн
├── DB/                     # Файлы базы данных
├── logs/                   # Логи Airflow
├── plugins/                # Плагины Airflow
├── S3/                     # Работа с S3 / MinIO
├── .gitignore
├── docker-compose.yaml     # Конфигурация Docker Compose
├── Dockerfile              # Образ приложения
├── README.md
└── requirements.txt        # Python-зависимости
```

## Запуск

Создать и настроить файл `.env`:

```bash
cd dags/practice
```

Запустить контейнеры:

```bash
docker compose up -d
```

Проверить состояние контейнеров:

```bash
docker compose ps
```

Посмотреть логи:

```bash
docker compose logs -f
```

Остановить проект:

```bash
docker compose down
```

## Основные компоненты

**Airflow** — оркестрация ETL-процесса.

**PostgreSQL** — хранение данных.

**S3 / MinIO** — объектное хранилище.

**Docker** — контейнеризация компонентов проекта.

**Python** — реализация ETL-логики.

## ETL

Общий процесс проекта:

```text
Источник данных
      ↓
    Extract
      ↓
   Transform
      ↓
     Load
      ↓
PostgreSQL / S3
```

## Цель

Практика построения ETL-пайплайна с использованием Apache Airflow, PostgreSQL, Docker и S3-совместимого хранилища.
