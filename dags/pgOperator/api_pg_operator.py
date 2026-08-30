import os
import requests
import psycopg2 as pg
from dotenv import load_dotenv
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

load_dotenv()


class APIToPgOperator(BaseOperator):

    API=os.getenv("API_URL")

    template_fields = ('date_from', 'date_end', 'sql_command')

    def __init__(self, sql_command, date_from, date_end, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.date_from = date_from
        self.date_end = date_end
        self.sql_command = sql_command


    def execute(self, context):
        payload = {
            'client': os.getenv('CLIENT'),
            'client_key': os.getenv('CLIENT_KEY'),
            'start': self.date_from,
            'end': self.date_end
        }

        try:
            response = requests.get(self.API, params=payload)
        except Exception as e:
            raise AirflowException(f'Error to connect: {e}')
        else:
            data = response.json()

        commands = (
            [self.sql_command]
            if isinstance(self.sql_command, str)
            else
            self.sql_command
        )

        skip_command = list()

        for indx, command in enumerate(commands):
            if command[:6].lower() == 'select':
                skip_command.append(indx)


        connection = BaseHook.get_connection('db_conn')

        with pg.connect(
            dbname='etl', sslmode='disable',
            user=connection.login, password=connection.password,
            port=connection.port, host=connection.host,
            connect_timeout=600, keepalives_idle=600, tcp_user_timeout=600
        ) as conn:
            cur = conn.cursor()

            for indx, command in enumerate(commands):
                if indx in skip_command:
                    continue
                cur.execute(command)

            conn.commit()
            cur.close()
