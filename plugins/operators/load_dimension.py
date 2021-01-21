from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This operator loads data from the staging tables into the dimension tables of the final dimensional model.
    
    param redshift_conn_id: Connection id of the Redshift connection. Default value 'redshift'
        type redshift_conn_id: str
    
    param table: Name of the table
        type aws_credentials_id: str

    param delete_load: Option to truncate existing tables. Default value 'False'
        type table_name: bool
        
    param sql_source: SQL querie to insert data into the table
        type table_name: str     
    """
    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 delete_load=False,
                 sql_source="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.delete_load = delete_load
        self.sql_source = sql_source

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_load:
            self.log.info("Truncating Redshift table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_source
        )
        self.log.info(f"Inserting into {self.table} ...")
        redshift.run(formatted_sql)