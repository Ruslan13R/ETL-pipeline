from datetime import datetime
from dotenv import load_dotenv
import os

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

load_dotenv()

DEFAULT_ARGS = {
    'owner': 'KhRG',
    'retiries': 2,
    'retry_delay': 60,
    'start_date': datetime(2026, 8, 9),
    'end_date': datetime(2026, 8, 31)
}

API_URL = os.getenv('API_URL')


def load_staging_schem(**context):
    import psycopg2 as pg
    import requests
    import ast
    import pendulum

    payload = {
        'client': os.getenv('CLIENT'),
        'client_key': os.getenv('CLIENT_KEY'),
        'start': context['ds'],
        'end': pendulum.parse(context['ds']).add(days=7).to_date_string()
    }

    response = requests.get(API_URL, params=payload)

    if response.status_code == 200:
        data = response.json()
    else:
        return f'status_code: {response.status_code}'

    connection = BaseHook.get_connection('db_conn')

    with pg.connect(
        dbname='etl',
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:
        cur = conn.cursor()

        for el in data:
            row = []
            passback_params = ast.literal_eval(el.get('passback_params', {}))
            row.append(el.get('lti_user_id'))
            row.append(passback_params.get('oauth_consumer_key'))
            row.append(passback_params.get('lis_result_sourcedid'))
            row.append(passback_params.get('lis_outcome_service_url'))
            row.append(True if el.get('is_correct') == 1 else False)
            row.append(el.get('attempt_type'))
            row.append(el.get('created_at'))

            cur.execute(
                '''INSERT INTO staging.stg_data VALUES(%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (lti_user_id, created_at) DO NOTHING''', row)

        conn.commit()
        cur.close()


def load_dds_schem(**context):
    import psycopg2 as pg

    sql_query = '''
        TRUNCATE TABLE dds.dds_data;

        INSERT INTO dds.dds_data
        SELECT
            lti_user_id, created_at::date AS date,
            COUNT(attempt_type) AS count_attempt,
            COUNT(is_correct) FILTER(WHERE is_correct IS NOT NULL AND is_correct=TRUE) AS count_is_correct,
            COUNT(attempt_type) FILTER(WHERE attempt_type='submit') AS count_submit
        FROM
            staging.stg_data
        GROUP BY
            lti_user_id, created_at::date;
    '''

    connection = BaseHook.get_connection('db_conn')

    with pg.connect(
        dbname='etl', sslmode='disable',
        user=connection.login, password=connection.password,
        host=connection.host, port=connection.port,
        connect_timeout=600, keepalives_idle=600, tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()

        cursor.execute(sql_query)

        cursor.close()


def load_to_s3(start_date, **context):
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
            date,
            count_attempt,
            count_is_correct,
            count_submit
        FROM
            dds.dds_data
        WHERE count_submit > 0 AND date >= { start_date }::date;
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

    # s3_client.create_bucket(
    #     Bucket='csv'
    # )

    s3_client.put_object(
        Body=file,
        Bucket='csv',
        Key=f"admin_{context['ds']}.csv"
    )


def load_stg_to_s3(**context):
    import csv
    import codecs
    import boto3 as s3
    import psycopg2 as pg
    from io import BytesIO
    from botocore.client import Config

    connection = BaseHook.get_connection('db_conn')

    query_stagin_sql = '''
        SELECT
            *
        FROM staging.stg_data;
    '''

    with pg.connect(
        dbname='etl', sslmode='disable',
        user=connection.login, password=connection.password,
        host=connection.host, port=connection.port,
        connect_timeout=600, keepalives_idle=600, tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(query_stagin_sql)

        stg_data = cursor.fetchall()

    stg_file = BytesIO()

    writer_wrapper_stg = codecs.getwriter('utf-8')
    writer_stg = csv.writer(
        writer_wrapper_stg(stg_file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )
    writer_stg.writerows(stg_data)

    stg_file.seek(0)

    connection = BaseHook.get_connection('s3_miniO')

    s3_client_stg =s3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version='s3v4')
    )

    s3_client_stg.put_object(
        Body=stg_file,
        Bucket='csv',
        Key=f"stg_{context['ds']}.csv"
    )


with DAG(
    dag_id='practice_task_k',
    tags=['KhRG', '3'],
    schedule='@weekly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_to_db = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_staging_schem
    )

    load_to_dds_db = PythonOperator(
        task_id='load_to_dds_db',
        python_callable=load_dds_schem
    )

    load_db_to_s3 = PythonOperator(
        task_id='load_to_s3',
        python_callable=load_to_s3,
        op_kwargs={
            'start_date': '{{ datetime(2026, 8, 10) }}'
        }
    )

    load_stg = PythonOperator(
        task_id='load_stg_to_s3',
        python_callable=load_stg_to_s3
    )

    dag_start >> load_to_db >> load_to_dds_db >> [load_stg, load_db_to_s3]
    [load_stg, load_db_to_s3] >> dag_end
