from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries



# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
"""
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 3, 29),
}
"""

dag = DAG('udac_dag',
          #description='Load and transform data in Redshift with Airflow',
          start_date=datetime.now()
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
"""
create_sparkify_table = PostgresOperator(
    task_id="create_sparkify_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.table_refresh
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_events",
    s3_bucket=Variable.get('s3_bucket'),
    s3_prefix="log_data",
    iam_role=Variable.get('iam_role'),
    js_format="s3://udacity-dend/log_json_path.json"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_songs",
    s3_bucket=Variable.get('s3_bucket'),
    s3_prefix="song_data",
    iam_role=Variable.get('iam_role'),
    js_format="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.user_table_insert
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.song_table_insert
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.artist_table_insert
)"""

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.time_table_insert
)
"""
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
"""
#start_operator>>create_sparkify_table
start_operator>>load_time_dimension_table

"""load_songplays_table>>load_user_dimension_table
start_operator>>create_sparkify_table
create_sparkify_table>>stage_events_to_redshift
create_sparkify_table>>stage_songs_to_redshift
stage_events_to_redshift>>load_songplays_table
stage_songs_to_redshift>>load_songplays_table
load_songplays_table>>load_song_dimension_table
load_songplays_table>>load_user_dimension_table
load_songplays_table>>load_artist_dimension_table
load_songplays_table>>load_time_dimension_table
load_song_dimension_table>>run_quality_checks
load_user_dimension_table>>run_quality_checks
load_artist_dimension_table>>run_quality_checks
load_time_dimension_table>>run_quality_checks
run_quality_checks>>end_operator
"""
