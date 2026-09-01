import psycopg2 as pg

from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator


class SqlSensor(BaseSensorOperator):
    template_fields = ('table_name',)

    def __init__(self, table_name, **kwargs):
        super().__init__(**kwargs)
        self.table_name = table_name

    def poke(self, context):
        connection = BaseHook.get_connection('db_conn')

        with pg.connect(
                dbname='etl', sslmode='disable',
                user=connection.login, password=connection.password, port=connection.port, host=connection.host,
                connect_timeout=600, keepalives_idle=600, tcp_user_timeout=600
            ) as conn:

            cur = conn.cursor()

            for table in self.table_name:
                sql_command = f"""
                    SELECT COUNT(1)
                    FROM {table}
                """
                cur.execute(sql_command)

                res = cur.fetchone()

                if res[0] == 0:
                    return False

            cur.close()
            return True
