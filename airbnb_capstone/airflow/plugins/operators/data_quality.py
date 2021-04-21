from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#C2FF33'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """
        Checking data quality of tables on the DB.
        1. Test for table names in the database
        2. Test that included tables have rows
        Params: 
          redshift_conn_id : Name of airflow connection id to a Redshift or Postgres DB
          tables : List of table names to test. 
        """

        self.log.info('Starting Data Quality Checks')
        
        data_quality_redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for tbl in self.tables:
            self.log.info(f"Checking Data Quality for {tbl} table")
            
            records = data_quality_redshift_hook.get_records(f'SELECT COUNT(*) from {tbl}')
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                raise ValueError(f'Data Quality check on table DIM_HOSTS failed')
            self.log.info(f'Data Quality Check Passed on {tbl} table with {records[0][0]} records!!!')
        
        self.log.info(f'------Data Quality passed on all the tables')

        """

    	## HOST TABLE
    	records = data_quality_redshift_hook.get_records(f'SELECT COUNT(*) from DIM_HOSTS WHERE HOST_ID IS NULL')
    	num_records = records[0][0]
    	if len(records) > 1 or len(records[0]) > 1 or num_records >= 1:
    		raise ValueError(f'Data Quality check on table DIM_HOSTS failed')
    	
    	logging.info(f"Data quality check on table DIM_HOSTS have completed successfully")

    	## REVIEWS TABLE
        records = data_quality_redshift_hook.get_records(f"SELECT COUNT(*) FROM DIM_REVIEWS WHERE REVIEW_ID IS NULL")
        num_records = records[0][0]
        if len(records) > 1 or len(records[0]) > 1 or num_records >= 1:
            raise ValueError(f"Data quality check on table DIM_REVIEWS failed")
        
        logging.info(f"Data quality check on table DIM_REVIEWS have completed successfully")
        
        ## CALENDARS TABLE
        records = data_quality_redshift_hook.get_records(f"SELECT COUNT(*) FROM DIM_CALENDARS WHERE REVIEW_ID IS NULL")
        num_records = records[0][0]
        if len(records) > 1 or len(records[0]) > 1 or num_records >= 1:
            raise ValueError(f"Data quality check on table DIM_CALENDARS failed")
        
        logging.info(f"Data quality check on table DIM_CALENDARS have completed successfully")
        

        ## PROPERTIES TABLE
        records = data_quality_redshift_hook.get_records(f"SELECT COUNT(*) FROM DIM_PROPERTIES WHERE REVIEW_ID IS NULL")
        num_records = records[0][0]
        if len(records) > 1 or len(records[0]) > 1 or num_records >= 1:
            raise ValueError(f"Data quality check on table DIM_PROPERTIES failed")
        
        logging.info(f"Data quality check on table DIM_PROPERTIES have completed successfully")
        

        ## FACT_AIRBNB_AUSTIN_LA TABLE
        records = data_quality_redshift_hook.get_records(f"SELECT COUNT(*) FROM FACT_AIRBNB_AUSTIN_LA WHERE FACT_ID IS NULL")
        num_records = records[0][0]
        if len(records) > 1 or len(records[0]) > 1 or num_records >= 1:
            raise ValueError(f"Data quality check on table FACT_AIRBNB_AUSTIN_LA failed")
        
        logging.info(f"Data quality check on table FACT_AIRBNB_AUSTIN_LA have completed successfully")

        """
