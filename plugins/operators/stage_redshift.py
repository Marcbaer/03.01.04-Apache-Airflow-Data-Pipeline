from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import datetime
from dateutil import parser

class StageToRedshiftOperator(BaseOperator):
    """
    This operator loads JSON data from a public Amazon S3 bucket into an Amazon Redshift cluster.
    
    param redshift_conn_id: Connection id of the Redshift connection. Default value 'redshift'
        type redshift_conn_id: str
    
    param aws_credentials_id: Connection id of the AWS credentials. Default value 'aws_credentials'
        type aws_credentials_id: str
        
    param table_name: Redshift staging table name
        type table_name: str
    
    param s3_bucket: Source Amazon S3 bucket
        type s3_bucket: str
    
    param json_path: Source Data JSON path. Default value 'auto'
        type json_path: str
        
    param use_partitioning: Option to load S3 data in partitions based on year and month of execution_date. Default value 'False'
        type use_partitioning: bool
        
    param execution_date: Logical execution date
        type execution_date: str
        
    """
    ui_color = '#358140'

    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        """

    TRUNCATE_SQL = """
        TRUNCATE TABLE {};
       """
    
    template_fields = ['execution_date']

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 table_name='',
                 s3_bucket='',
                 json_path='auto',
                 use_partitioning=False,
                 execution_date="",
                 truncate_table=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        self.execution_date = execution_date
        self.use_partitioning = use_partitioning
        self.truncate_table = truncate_table

    def execute(self, context):
        execution_date = parser.parse(self.execution_date)
        self.log.info(f"Execution date: {execution_date}")
        self.log.info(f"Execution year: {execution_date.year}")
        self.log.info(f"Execution month: {execution_date.month}")
        self.log.info(f"Use Partitioning: {self.use_partitioning}")

        # If partitioning is applied: year and month of execution_date
        s3_path = self.s3_bucket+f'/{execution_date.year}/{execution_date.month}' if self.use_partitioning else self.s3_bucket
        self.log.info(f"s3 path: {s3_path}")
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info("Truncating Redshift table {self.table_name}")
            redshift_hook.run(self.TRUNCATE_SQL.format(self.table_name))    
            #redshift.run("TRUNCATE TABLE {}".format(self.table_name))
            
        sql_stmt = self.COPY_SQL.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )

        self.log.info(f"Start copying data to staging table: {self.table_name}")
        redshift_hook.run(sql_stmt)
        self.log.info(f"Staging table completed: {self.table_name}")