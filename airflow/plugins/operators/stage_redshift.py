from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    template_fields = ('s3_key',)
    ui_color = '#358140'
    
    copy_sql_query = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        format as json '{}';
    """

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 file_type="",                 
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_type = file_type
        self.json_path = json_path

    def execute(self, context):
        """ 
        Copies JSON/ CSV data files stored in AWS S3 and transfer them into AWS Redshift cluster.
        PARAMS:
            conn_id = name of Airflow connection id to Redshift DB
            aws_credentials_id = name of Airflow connection details to AWS 
            table = name of destination table on AWS Redshift -- Redshift Id on Airflow
            s3_bucket= name of AWS S3 bucket source
            s3_key = path of sour file. 
            region = Region of AWS S3 and Redshift DB. Must be in the same region.
            file_type = file type to look for in S3. JSON or CSV.
            json_path = string of file path for explicit JSON mapping. Must be s3 key formated to map
                        JSON data keys to destination columns in the redshfit staging table. 
                        Default to 'auto', which will map matching keys to table columns.
        """
        
        self.log.info('StageToRedshiftOperator not implemented yet')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        
        self.log.info('Clearing data from destination Redshift table')
        redshift.run(f'DELETE FROM {self.table}')
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        
        full_s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        if self.json_path != "":
            self.json_path = "s3://{}/{}".format(self.s3_bucket, self.json_path)
            formatted_sql = self.copy_sql_query.format(self.table, 
                                                       full_s3_path, 
                                                       credentials.access_key, credentials.secret_key, 
                                                       self.json_path)
        else:
            formatted_sql = self.copy_sql_query.format(self.table, 
                                                       full_s3_path, 
                                                       credentials.access_key, credentials.secret_key, 'auto')
        '''
        self.s3_bucket = self.s3_bucket+self.s3_key
        formatted_sql = self.COPY_SQL.format(
            self.table,
            self.s3_bucket,
            credentials.access_key,
            credentials.secret_key,
            self.json_path)
        '''
        
        self.log.info(f"Running copy query : {formatted_sql}")
        redshift.run(formatted_sql)
        self.log.info(f"Staging of Table {self.table} Completed Successfully")
                
                     
                     





