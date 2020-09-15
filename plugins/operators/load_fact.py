from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_stmt ="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.table = table

    def execute(self, context):
        self.log.info(f'LoadFactOperator Executing Query : {self.sql_stmt}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'LoadFactOperator : Inserting data to {self.table}')
        insert_stmt = f'INSERT INTO {self.table} {self.sql_stmt}'
        redshift.run(insert_stmt)