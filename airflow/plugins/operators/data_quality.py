from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id ="",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.tables  = tables

    def execute(self, context):
        """
        Checking data quality of tables on the DB.
        1. Test for table names in the database
        2. Test that included tables have rows
        Params: 
          conn_id : Name of airflow connection id to a Redshift or Postgres DB
          tables : List of table names to test. 
        """
        self.log.info(f'DataQualityOperator Starting with {self.conn_id}')
        #self.log.info('DataQualityOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        
        for tbl in self.tables:
            self.log.info(f"Checking Data Quality for {tbl} table")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {tbl}")       
            
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data quality check failed: {tbl} Table has no results")
                raise ValueError(f"Data quality check failed: {tbl} Table has no results")
            self.log.info(f"Data Quality Check Passed on {tbl} table with {records[0][0]} records!!!")
        
        
                      
        self.log.info(f'------DataQualityOperator {self.conn_id} passed')        
                

                