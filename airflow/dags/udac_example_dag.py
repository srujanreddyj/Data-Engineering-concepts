from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

def start():
    logging.info("Start of DAG")
    
def end():
    logging.info("End of DAG")

def lit_keys():
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = Variable.get('s3_bucket')
    
    keys = hook.list_keys(bucket)
    for key in keys:
        logging.info(f"- Listing Keys from  s3://{key}")

default_args = {
    'owner': 'srujan',
    'start_date': datetime(2018, 1, 11),
    'depends_on_past': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG('Sparkify-Data-Pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs = 3,
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_tables
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    provide_context=True,
    dag=dag,
    aws_credentials_id = 'aws_credentials',
    conn_id='redshift',
    table='staging_events',
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path='log_json_path.json',
    file_type='JSON'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    aws_credentials_id = 'aws_credentials',
    conn_id = 'redshift',
    table = 'staging_songs',
    s3_bucket="udacity-dend",
    s3_key="song_data",
    file_type='JSON'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    conn_id = 'redshift',
    table='songplays',
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
    conn_id = 'redshift',
    table='users',
    query=SqlQueries.user_table_insert,
    insert_mode='truncate-insert'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
    conn_id = 'redshift',
    table='songs',
    query=SqlQueries.song_table_insert,
    insert_mode='truncate-insert'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
    conn_id = 'redshift',
    table='artists',
    query=SqlQueries.artist_table_insert,
    insert_mode='truncate-insert'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    conn_id='redshift',
    table='time',
    query=SqlQueries.time_table_insert,
    insert_mode='truncate-insert'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    conn_id = 'redshift',
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, 
                   stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table,
                         load_artist_dimension_table,
                         load_song_dimension_table,
                         load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
