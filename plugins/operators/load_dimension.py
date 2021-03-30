from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 log="",
                 isAppend=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql=sql
        self.logging=log
        self.isAppend=isAppend
        
    def execute(self, context):
        logging.info(self.logging)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.isAppend:
            self.sql=self.sql.split(";")[1]
        redshift.run(self.sql)
        logging.info("Success")
        