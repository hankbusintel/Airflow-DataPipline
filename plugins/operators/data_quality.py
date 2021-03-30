from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import json

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 log="",
                 db_check="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.logging=log
        self.db_check=db_check
    
    def checkdbresult(self,redshift,sql,expect_result):
        records = redshift.get_records(sql)
        num_records = records[0][0]
        if num_records == expect_result:
            logging.info("Check cases passed.")
        else:           
            raise ValueError(f"Check Faield. There are {num_records} 'null' in the table. \
                         Expected value is {expect_result}")
        
    def execute(self, context):
        logging.info(self.logging)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        db_check = list(self.db_check.split("|"))
        for item in db_check:
            jsonitem=json.loads(item)
            sql= jsonitem["check_sql"]
            expect_result=jsonitem["expected_result"]
            logging.info(f"checking query: {sql} ")
            self.checkdbresult(redshift,sql,expect_result)  
      