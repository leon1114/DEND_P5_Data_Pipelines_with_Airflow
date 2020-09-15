from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_stmt=sql_stmt
        self.append_data=append_data

    def execute(self, context):
        self.log.info(f'LoadDimensionOperator Executing Query : {self.sql_stmt} (Append_data : {self.append_data})')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data == False:
            self.log.info(f'LoadDimensionOperator : Deleting from {self.table}')
            delete_stmt = f'DELETE FROM {self.table}'
            redshift.run(delete_stmt)
        self.log.info(f'LoadDimensionOperator : Inserting data to {self.table}')
        insert_stmt = f'INSERT INTO {self.table} {self.sql_stmt}'
        redshift.run(insert_stmt)
