from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators.create_tables import CreateTablesOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from helpers.sql_queries import SqlQueries

from airflow.operators.subdag_operator import SubDagOperator
from subdag import create_load_quality


default_args = {
    'owner': 'Pedro Couto',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email': ['pedrocouto39@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('sparkify_pipeline',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@monthly',
          max_active_runs =  1)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_stage_events_table = CreateTablesOperator(
    task_id = 'Create_stage_events_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    target_schema = 'public',
    table = 'events')

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_conn_id = 'aws_credentials', 
    table = 'events',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    json_type = 's3://udacity-dend/log_json_path.json')

create_stage_songs_table = CreateTablesOperator(
    task_id = "Create_stage_songs_table",
    dag = dag,
    redshift_conn_id = 'redshift',
    target_schema = 'public',
    table = 'songs')

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_conn_id = 'aws_credentials', 
    table = 'songs',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data/A/A/A',
    json_type = 'auto')

create_fact_table = CreateTablesOperator(
    task_id = 'Create_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    target_schema = 'public',
    table = 'songplays')

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    source_schema = 'public',
    target_schema = 'public',
    table = 'songplays')

task_id_user_table = 'create_load_check_user_table_subdag'
user_subdag_task = SubDagOperator(
    subdag = create_load_quality(
        'sparkify_pipeline',
        task_id_user_table,
        'redshift',
        'public',
        'public',
        'user',
        False,
        None,
        ['SELECT count(*) FROM {}'],
        [0],
        start_date = default_args['start_date'],
    ),
    task_id = task_id_user_table,
    dag = dag)

task_id_song_table = 'create_load_check_song_table_subdag'
song_subdag_task = SubDagOperator(
    subdag = create_load_quality(
        'sparkify_pipeline',
        task_id_song_table,
        'redshift',
        'public',
        'public',
        'song',
        False,
        None,
        ['SELECT count(*) FROM {}'],
        [0],
        start_date = default_args['start_date'],
    ),
    task_id = task_id_song_table,
    dag = dag)

task_id_artist_table = 'create_load_check_artist_table_subdag'
artist_subdag_task = SubDagOperator(
    subdag = create_load_quality(
        'sparkify_pipeline',
        task_id_artist_table,
        'redshift',
        'public',
        'public',
        'artist',
        True,
        'artist_id',
        ['SELECT count(*) FROM {}'],
        [0],
        start_date = default_args['start_date'],
    ),
    task_id = task_id_artist_table,
    dag = dag)

task_id_time_table = 'create_load_check_time_table_subdag'
time_subdag_task = SubDagOperator(
    subdag = create_load_quality(
        'sparkify_pipeline',
        task_id_time_table,
        'redshift',
        'public',
        'public',
        'time',
        False,
        None,
        ['SELECT count(*) FROM {}'],
        [0],
        start_date = default_args['start_date']
    ),
    task_id = task_id_time_table,
    dag = dag)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Dependencies setup to create the workflow
start_operator >> create_stage_events_table >> stage_events_to_redshift
start_operator >> create_stage_songs_table >> stage_songs_to_redshift

[stage_events_to_redshift, stage_songs_to_redshift] >> create_fact_table >> load_songplays_table

load_songplays_table >> [user_subdag_task,
                        song_subdag_task,
                        artist_subdag_task,
                        time_subdag_task] >> end_operator


