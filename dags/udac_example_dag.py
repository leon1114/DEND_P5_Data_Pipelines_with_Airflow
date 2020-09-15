from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

aws_hook = AwsHook('aws_credentials')
credentials = aws_hook.get_credentials()
access_key = credentials.access_key
secret_key = credentials.secret_key

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020,9,12),#datetime(2019, 1, 12),
    #'retries' : 3,
    #'retry_delay' : timedelta(minutes=5),
    'email_on_retry' : False,
    'depends_on_past' : False,
    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    access_key=access_key,
    secret_key=secret_key,
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    access_key=access_key,
    secret_key=secret_key,
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt=SqlQueries.songplay_table_insert,
    table='songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt=SqlQueries.user_table_insert,
    table='users',
    append_data=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt=SqlQueries.song_table_insert,
    table='songs',
    append_data=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt=SqlQueries.artist_table_insert,
    table='artists',
    append_data=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt=SqlQueries.time_table_insert,
    table='time',
    append_data=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tests = [
        {'check_sql': "SELECT COUNT(*) FROM users", 'expected_result': 0, 'check_equality' : False},
        {'check_sql': "SELECT COUNT(*) FROM songs", 'expected_result': 0, 'check_equality' : False},
        {'check_sql': "SELECT COUNT(*) FROM artists", 'expected_result': 0, 'check_equality' : False},
        {'check_sql': "SELECT COUNT(*) FROM time", 'expected_result': 0, 'check_equality' : False},
        {'check_sql': "SELECT COUNT(*) FROM songplays", 'expected_result': 0, 'check_equality' : False},
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
