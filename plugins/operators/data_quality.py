from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    data_quality=("""
        SELECT COUNT(*) FROM songplays
        WHERE (songid = '') IS NOT FALSE OR
        (artistid = '') IS NOT FALSE OR
        (CAST(userid as VARCHAR) = '') IS NOT FALSE 
    """)
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift.get_records(DataQualityOperator.data_quality)
        num_records = records[0][0]
        if num_records > 0 :
            raise ValueError(f"Data quality check failed. There are {num_records} 'null' in songplays table")
        logging.info(f"Data quality on table check with {num_records} records")