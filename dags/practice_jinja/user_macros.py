from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

from dotenv import load_dotenv
from datetime import datetime, timedelta
import os

load_dotenv()


DEFAULT_ARGS = {
    'owner': 'KhRG',
    'retry': 2,
    'retry_delay': 60,
    'start_date': datetime(2026, 7, 1)
}

API_URL = os.getenv('API_URL')


def start_date_month(date, day=1):
    date = datetime.strptime(date,'%Y-%m-%d')

    return datetime(date.year, date.month, day)


def end_date_month(date):
    currently_month = datetime.now().month
    date = datetime.strptime(date,'%Y-%m-%d')

    if date.month == 2:
        if ((date.year % 4 == 0 and date.year % 100 != 0) or date.year % 400 == 0):
            return datetime(date.year, date.month, 29)
        return  datetime(date.year, date.month, 28)

    elif date.month == currently_month:
        return datetime.now() - timedelta(days=1)


    return datetime(date.year, date.month, 30 + (date.month + date.month // 8) % 2)


def load_staging_month(start_date, end_date, **context):
    import psycopg2 as pg
    import requests
    import ast

    payload = {
        'client': os.getenv('CLIENT'),
        'client_key': os.getenv('CLIENT_KEY'),
        'start': start_date,
        'end': end_date
    }

    try:
        response = requests.get(API_URL, params=payload)
    except Exception as e:
        print(f'Error: {e}')
    else:
        data = response.json()

    connection = BaseHook.get_connection('db_conn')

    with pg.connect(
            dbname='etl', sslmode='disable',
            user=connection.login, password=connection.password,
            host=connection.host, port=connection.port,
            connect_timeout=600, keepalives_idle=600, tcp_user_timeout=600
        ) as conn:
        cur = conn.cursor()

        for el in data:
            row = []
            try:
                passback_params = ast.literal_eval(el.get('passback_params', {}))
            except Exception as e:
                print(f'Doesn`t passback_params: {e}')

            row.append(el.get('lti_user_id'))
            row.append(passback_params.get('oauth_consumer_key'))
            row.append(passback_params.get('lis_result_sourcedid'))
            row.append(passback_params.get('lis_outcome_service_url'))
            row.append(True if el.get('is_correct') == 1 else False)
            row.append(el.get('attempt_type'))
            row.append(el.get('created_at'))
            row.append('KhRG')

            cur.execute(
                '''INSERT INTO staging.stg_month_data VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (lti_user_id, created_at) DO NOTHING''', row
            )

        conn.commit()
        cur.close()


def load_to_s3(logical_date, filename, **context):
    from io import BytesIO
    from botocore.client import Config
    import psycopg2 as pg
    import boto3 as s3
    import codecs
    import csv

    connection = BaseHook.get_connection('db_conn')

    query_sql = f'''
        SELECT
            lti_user_id,
            oauth_consumer_key,
            created_at,
            user_create
        FROM staging.stg_month_data
        WHERE EXTRACT(MONTH FROM created_at) = { logical_date };
    '''

    with pg.connect(
            dbname='etl', sslmode='disable',
            user=connection.login, password=connection.password,
            host=connection.host, port=connection.port,
            connect_timeout=600, keepalives_idle=600, tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(query_sql)

        data = cursor.fetchall()

    file = BytesIO()

    writer_wrapper = codecs.getwriter('utf-8')

    writer = csv.writer(
        writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )
    writer.writerows(data)

    file.seek(0)

    connection = BaseHook.get_connection('s3_miniO')

    s3_client = s3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version='s3v4')
    )

    s3_client.put_object(
        Body=file,
        Bucket='csv',
        Key=filename
    )


with DAG(
    dag_id='pr_2-jinja',
    tags=['KhRG', '3'],
    schedule='@monthly',
    default_args=DEFAULT_ARGS,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True,
    user_defined_macros={'start_month': start_date_month, 'end_month': end_date_month}
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_staging_schem_jinja = PythonOperator(
        task_id='load_staging_schem_jinja',
        python_callable=load_staging_month,
        op_kwargs={
            'start_date': '{{ start_month(ds) }}',
            'end_date': '{{ end_month(ds) }}'
        }
    )

    load_to_minio = PythonOperator(
        task_id='load_to_s3',
        python_callable=load_to_s3,
        op_kwargs={
            'logical_date': '{{ logical_date.month }}',
            'filename': 'date_of_{{ logical_date.month }}_month.csv'
        }
    )

    dag_start >> [load_staging_schem_jinja, load_to_minio]
    [load_staging_schem_jinja, load_to_minio] >> dag_end