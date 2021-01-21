from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 1, 18),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
    'depends_on_past': False,
    'schedule_interval': '@monthly'
}

dag = DAG('S3_to_Redshift_ETL',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_logdata",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table_name='public.staging_events',
    s3_bucket='s3://udacity-dend/log_data',
    json_path = 's3://udacity-dend/log_json_path.json',
    use_paritioning = False,
    execution_date = '{{ execution_date }}',
    truncate_table=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="stage_songdata",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table_name='public.staging_songs',
    s3_bucket='s3://udacity-dend/song_data',
    json_path = 'auto',
    use_paritioning = False,
    execution_date = '{{ execution_date }}',
    truncate_table=True
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="public.songplays",
    sql_source=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    redshift_conn_id="redshift",
    table="public.users",
    sql_source=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    redshift_conn_id="redshift",
    table="public.songs",
    sql_source=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    redshift_conn_id="redshift",
    table="public.artists",
    sql_source=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    redshift_conn_id="redshift",
    table="public.time",
    sql_source=SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_check_queries=["SELECT COUNT(*) FROM songs", \
                       "SELECT COUNT(*) FROM songplays", \
                       "SELECT COUNT(*) FROM artists", \
                       "SELECT COUNT(*) FROM artists", \
                       "SELECT COUNT(*) FROM time" \
                      ],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator>>create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift
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