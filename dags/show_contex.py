from airflow.models.dag import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

DEFAULT_ARGS = {
    'owner': 'Ruslan',
    'retries': 2,
    'retry_delay': 60,
    'start_date': datetime(2026, 8, 29),
    'end_date': datetime(2026, 8, 30)
}


def about_context(**context):
    for key, value in context.items():
        print(f'key: {key}')
        print(f'value: {value}')
        print(f'Type key: {type(key)}\nType value: {type(value)}')


with DAG(
    dag_id='show_context',
    tags=['3', 'KhRG'],
    schedule='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    about = PythonOperator(
        task_id='about_context',
        python_callable=about_context
    )

    dag_start >> about >> dag_end