#Commit check
from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    "owner": "udacity",
    'start_date': pendulum.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}

# Add the AWS S3 Bucket information:
S3_BUCKET = "cibarra"
S3_REGION  = "us-east-1"
S3_LOG_PREFIX  = "log-data"
S3_SONG_PREFIX = "song-data"
LOG_JSONPATH  = f"s3://cibarra/log_json_path.json"

# Authentication info
AWS_CONN_ID = "aws_credentials"
REDSHIFT_CONN_ID = "redshift"
IAM_ROLE_ARN = None 


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval="@hourly",
    catchup=False,                 
    max_active_runs=1,
    tags=["udacity"]
)

def final_project():
    start_execution = DummyOperator(task_id="start_execution")
    end_execution   = DummyOperator(task_id="end_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="stage_events",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CONN_ID,
        table="staging_events",
        s3_bucket=S3_BUCKET,
        s3_key=S3_LOG_PREFIX,          
        region=S3_REGION,
        json_option=LOG_JSONPATH,     
        truncate=True
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="stage_songs",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CONN_ID,
        table="staging_songs",
        s3_bucket=S3_BUCKET,
        s3_key=S3_SONG_PREFIX,
        region=S3_REGION,
        json_option="auto",          
        truncate=True
    )

    load_songplays_table = LoadFactOperator(
        task_id="load_songplays_fact_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="songplays",
        insert_sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="load_user_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="users",
        insert_sql=SqlQueries.user_table_insert,
        append_only=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="load_song_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="songs",
        insert_sql=SqlQueries.song_table_insert,
        append_only=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="load_artist_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="artists",
        insert_sql=SqlQueries.artist_table_insert,
        append_only=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="load_time_dim_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="time",
        insert_sql=SqlQueries.time_table_insert,
        append_only=False
    )

    run_quality_checks = DataQualityOperator(
        task_id="run_data_quality_checks",
        redshift_conn_id=REDSHIFT_CONN_ID,

        tables=["songplays", "users", "songs", "artists", "time"]
        
    )

    # Dependencies for reference on dag flow
    start_execution >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks >> end_execution

final_project_dag = final_project()
