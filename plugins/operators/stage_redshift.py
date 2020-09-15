from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        region 'us-west-2';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 access_key="",
                 secret_key="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.access_key = access_key
        self.secret_key = secret_key
        self.json_path = json_path

    def execute(self, context):
        self.log.info("StageToRedshiftOperator Starts")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.access_key,
            self.secret_key,
            self.json_path
        )
        self.log.info(f"Running {formatted_sql}")
        redshift.run(formatted_sql)
        


