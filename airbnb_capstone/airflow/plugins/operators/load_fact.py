from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator (BaseOperator):
    
    ui_color = '#33C9FF'
    load_sql_fact_template = "{}"
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 query = "",
                 operation = "",
                 table = "",
                 *args, **kwargs):
        
        
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.query = query
        self.operation = operation
        self.table = table

    def execute(self, context):
        """
        Load data from redshift staging table to a fact table 
        
        PARAMS:
        redshift_conn_id = name of the connection id in Airflow to Redshift
        aws_credentials_id = name of Airflow connection details to AWS 
        table = name of the destination table
        query = SQL query to be executed on the DB
        operation = mode of operation. Insert or Delete tables
        self.log.info('LoadFactOperator not implemented yet')
        
        """


        self.log.info('Starting Load Fact Table')
        sql_redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        fact_load_query = LoadFactOperator.load_sql_fact_template.format(self.query)
        self.log.info('Running the Load Fact Command')
        if self.operation == 'insert':
            #sql_redshift_hook.run(fact_load_query)
            sql_redshift_hook.run(LoadFactOperator.load_sql_fact_template.format(self.query))
        else:
            sql_redshift_hook.run(f'DELETE FROM {self.table}')
            
        self.log.info('Load Fact Table Complete')

