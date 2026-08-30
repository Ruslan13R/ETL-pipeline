from pgOperator.api_pg_operator import APIToPgOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.dag import DAG
from datetime import datetime

DEFAULT_ARGS = {
    'owner': 'KhRG',
    'retries': 2,
    'retry_delay': 60,
    'start_date': datetime(2026, 8, 25)
}

commands = [
    'DROP TABLE IF EXISTS staging.testData;',
    'CREATE TABLE IF NOT EXISTS staging.testData (id INT NOT NULL, name TEXT);',
    "INSERT INTO staging.testData VALUES(1, 'R'), (2, 'RR'), (3, 'RRR');",
    "SELECT * FROM staging.testData;"
]

with DAG(
    dag_id='api_to_pg_operator', default_args=DEFAULT_ARGS,
    tags=['3', 'KhRG'], schedule='@once',
    max_active_runs=1, max_active_tasks=1, render_template_as_native_obj=True
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    topg = APIToPgOperator(
        task_id='pgOperator',
        sql_command=commands,
        date_from='{{ ds }}',
        date_end='{{ tomorrow_ds }}'
    )

    dag_start >> topg >> dag_end