from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    #template_fields = ('s3_key',)
    ui_color = '#E6B0AA'

    load_stage_copy_sql_csv = """
        COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' {} IGNOREHEADER 1;
    """
    
    load_stage_copy_sql_parquet = """
        COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' FORMAT AS PARQUET;
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_path="",
                 method="",
                 region='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.method = method
        self.region = region

    def execute(self, context):
        """ 
        Copies Parquet/ CSV data files stored in AWS S3 and transfer them into AWS Redshift cluster.
        PARAMS:
            redshift_conn_id = name of Airflow connection id to Redshift DB
            aws_credentials_id = name of Airflow connection details to AWS 
            table = name of destination table on AWS Redshift -- Redshift Id on Airflow
            s3_bucket= name of AWS S3 bucket source
            s3_path = path of sour file. 
            region = Region of AWS S3 and Redshift DB. Must be in the same region.
            method = file type to look for in S3. Parquet or CSV.
        """


        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        stage_redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from destination Redshift table')
        stage_redshift_hook.run(f'DELETE FROM {self.table}')

        self.log.info("Starting staging data from S3 to Redshift")
        #rendered_key = self.s3_key.format(**context)

        s3_path_full = "s3://{}/{}".format(self.s3_bucket, self.s3_path)

        method = self.method
        
        if method != '':
            stage_formatted_sql = StageToRedshiftOperator.load_stage_copy_sql_csv.format(
                self.table, 
                s3_path_full,
                credentials.access_key, 
                credentials.secret_key, 
                self.method
            )
        else:            
            stage_formatted_sql = StageToRedshiftOperator.load_stage_copy_sql_parquet.format(
                self.table, 
                s3_path_full,
                credentials.access_key, 
                credentials.secret_key
            )
            
            
            

        self.log.info(f"Running copy query : {stage_formatted_sql}")
        stage_redshift_hook.run(stage_formatted_sql)

        self.log.info("Staging data uploaded from S3 to Redshift")

        self.log.info(f"Staging of Table {self.table} Completed Successfully")
