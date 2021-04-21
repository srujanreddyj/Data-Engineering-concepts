from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTablesOperator(BaseOperator):
    ui_color = '#F98866'
    
    create_sql = """{{ }}"""
    
    @apply_defaults
    def __int__(self,
                aws_credentials_id = "",
                redshift_conn_id = "",
                query = "",
                table = "",
                *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def excute(self, context):
        """
        Loads data from staging tables into dimension tables in AWS redshift.
        PARAMS:
        redshift_conn_id = name of connection id stored in redshift for Airflow to connect
        query = string of SQL query to be used to CREATE tables
        table = name of destination table 
        """

        self.log.info('Starting Creation of %s Table', self.table)
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        create_sql_query = CreateTablesOperator.create_sql.format(self.query)
        
        self.log.info(f"Running create query : {create_sql_query}")
        
        redshift_hook.run(create_sql_query)
        
        self.log.info(f"Creating of Table {self.table} Completed Successfully")
