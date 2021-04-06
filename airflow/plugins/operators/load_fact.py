from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    load_sql = """
        INSERT INTO {} ({});
    """

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 table="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        """
        Load data from redshift staging table to a fact table 
        
        PARAMS:
        conn_id = name of the connection id in Airflow to Redshift
        table = name of the destination table
        query = SQL query to be executed on the DB
        self.log.info('LoadFactOperator not implemented yet')
        
        """
        self.log.info("LoadFactOperator Starting")
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        
        redshift.run(LoadFactOperator.load_sql.format(self.table, self.query))
        
        self.log.info('LoadFactOperator Complete')
