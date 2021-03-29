from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    sql_copy = """
        copy {} from 's3://{}/{}'
        credentials 'aws_iam_role={}'
        json '{}'
        compupdate on region 'us-west-2';
    """
    


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 js_format="",
                 s3_bucket="",
                 s3_prefix="",
                 iam_role="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.iam_role=iam_role
        self.aws_credentials_id = aws_credentials_id
        self.js_format=js_format
        

        
        
    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        #self.log.info("AWS credentials: "+self.aws_credentials_id)
        #self.log.info("redshift: "+self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = StageToRedshiftOperator.sql_copy.format(
            self.table,
            self.s3_bucket,
            self.s3_prefix,
            self.iam_role,
            self.js_format
        )
        #self.log.info(formatted_sql)
        redshift.run(formatted_sql)


