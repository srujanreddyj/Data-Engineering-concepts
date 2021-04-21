from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
#import sql


start_date = datetime.utcnow()

default_args = {
    'owner': 'srujan',
    'start_date': start_date,
    'depends_on_past': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG('Airbnb-Data-Pipeline',
          default_args=default_args,
          description='Load and transform Airbnb data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=3,
          catchup=False
          )

start_operator = DummyOperator(task_id='START_OPERATOR', dag=dag)

create_staging_listings_table = PostgresOperator(
    task_id="Create_STAGGING_Listings_Table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_listings
)

create_staging_listings_table.set_upstream(start_operator)
## Creating Stagging Tables - DAGs

create_staging_calendar_table = PostgresOperator(
    task_id="Create_STAGGING_Calendar_Table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_calendar
)

create_staging_calendar_table.set_upstream(start_operator)

create_staging_reviews_table = PostgresOperator(
    task_id="Create_STAGGING_Reviews_Table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_reviews
)

create_staging_reviews_table.set_upstream(start_operator)


##Loading original data into Stagging Tables - DAGs

stage_listings_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Listings',
    provided_context=True,
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_listings',
    s3_bucket='udcdecapstone',
    s3_path='airbnb_listing_austin_la.parquet',
    region="us-east-1"
)

stage_listings_to_redshift.set_upstream(create_staging_listings_table)

stage_calendars_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Calendar',
    provided_context=True,
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_calendar',
    s3_bucket='udcdecapstone',
    s3_path='airbnb_calender_austin_la.csv',
    method='csv',
    region="us-east-1"
)

stage_calendars_to_redshift.set_upstream(create_staging_calendar_table)

stage_reviews_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Reviews',
    provided_context=True,
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_reviews',
    s3_bucket='udcdecapstone',
    s3_path='airbnb_reviews_austin_la.parquet',
    region="us-east-1"
)

stage_reviews_to_redshift.set_upstream(create_staging_reviews_table)

## Dummy Operator waiting for every staging table to load successfully - DAGs

MID_operator = DummyOperator(task_id='MID_OPERATOR',  dag=dag)

MID_operator.set_upstream(stage_listings_to_redshift)
MID_operator.set_upstream(stage_calendars_to_redshift)
MID_operator.set_upstream(stage_reviews_to_redshift)


##Creating FACT and DIMENSIONS Tables - DAGS

create_table_dim_hosts = PostgresOperator(
    task_id="Create_Table_DIM_HOSTS",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_dim_hosts
)

create_table_dim_hosts.set_upstream(MID_operator)

create_table_dim_reviews = PostgresOperator(
    task_id="Create_Table_DIM_REVIEWS",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_dim_reviews
)

create_table_dim_reviews.set_upstream(MID_operator)


create_table_dim_calendars = PostgresOperator(
    task_id="Create_Table_DIM_CALENDAR",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_dim_calendars
)
create_table_dim_calendars.set_upstream(MID_operator)


create_table_dim_properties = PostgresOperator(
    task_id="Create_Table_DIM_PROPERTIES",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_dim_properties
)
create_table_dim_properties.set_upstream(MID_operator)

##Loading events into STG Tables - DAGS

load_table_dim_hosts = LoadDimensionOperator(
    task_id='LOAD_TABLE_DIM_HOSTS',
    provided_context=True,
    dag=dag,
    redshift_conn_id='redshift',
    query=SqlQueries.hosts_table_insert,
    operation='insert',
    table='DIM_HOSTS'
)
load_table_dim_hosts.set_upstream(create_table_dim_hosts)

load_table_dim_calendars = LoadDimensionOperator(
    task_id='LOAD_TABLE_DIM_CALENDARS',
    provided_context=True,
    dag=dag,
    redshift_conn_id='redshift',
    query=SqlQueries.calendars_table_insert,
    operation='insert',
    table='DIM_CALENDARS'
)
load_table_dim_calendars.set_upstream(create_table_dim_calendars)

load_table_dim_reviews = LoadDimensionOperator(
    task_id='LOAD_TABLE_DIM_REVIEWS',
    provided_context=True,
    dag=dag,
    redshift_conn_id='redshift',
    query=SqlQueries.reviews_table_insert,
    operation='insert',
    table='DIM_REVIEWS'
)
load_table_dim_reviews.set_upstream(create_table_dim_reviews)

load_table_dim_properties = LoadDimensionOperator(
    task_id='LOAD_TABLE_DIM_PROPERTIES',
    provided_context=True,
    dag=dag,
    redshift_conn_id='redshift',
    query=SqlQueries.properties_table_insert,
    operation='insert',
    table='DIM_PROPERTIES'
)
load_table_dim_properties.set_upstream(create_table_dim_properties)


#Creating Fact Table
create_table_fact_airbnb = PostgresOperator(
    task_id="Create_Table_FACT_AIRBNB",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_fact_airbnb
)

load_table_fact_airbnb_austin_la = LoadFactOperator(
    task_id='Load_Fact_Airbnb_Austin_LA_Table',
    provided_context=True,
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    query=SqlQueries.load_fact_airbnb_austin_la_insert,
    operation='insert',
    table='FACT_AIRBNB_AUSTIN_LA'
)

create_table_fact_airbnb.set_upstream(load_table_dim_hosts)
create_table_fact_airbnb.set_upstream(load_table_dim_properties)
create_table_fact_airbnb.set_upstream(load_table_dim_calendars)
create_table_fact_airbnb.set_upstream(load_table_dim_reviews)

load_table_fact_airbnb_austin_la.set_upstream(create_table_fact_airbnb)

## RUN DATA QUALITY CHECKS TO ENSURE Recors have been moved correctly through platforms without any errors
run_quality_checks = DataQualityOperator(
    task_id='Run_DATA_QUALITY_CHECKS',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    tables=['DIM_HOSTS', 'DIM_REVIEWS', 'DIM_CALENDARS',
            'DIM_PROPERTIES', 'FACT_AIRBNB_AUSTIN_LA']
)

run_quality_checks.set_upstream(load_table_fact_airbnb_austin_la)

end_operator = DummyOperator(task_id='END_TASK', dag=dag)

end_operator.set_upstream(run_quality_checks)


'''
start_operator >> [create_staging_listings_table, 
				   create_staging_calendar_table,
				   create_staging_reviews_table] >> [stage_listings_to_redshift, 
				   									stage_calendars_to_redshift,
				   									stage_reviews_to_redshift] >> MID_operator

MID_operator >> [create_table_dim_hosts, 
				create_table_dim_properties,
				create_table_dim_reviews,
				create_table_dim_calendar] >> [load_table_dim_hosts,
											   load_table_dim_properties,
											   load_table_dim_reviews,
											   load_table_dim_calendars] >> create_load_fact_airbnb_austin_la_table


create_load_fact_airbnb_austin_la_table >> load_table_fact_airbnb_austin_la >> run_quality_checks >> end_operator			  
'''
