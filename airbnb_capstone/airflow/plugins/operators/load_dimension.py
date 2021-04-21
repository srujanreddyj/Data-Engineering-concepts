from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator (BaseOperator):
	ui_color = '#3348FF'
	load_sql_template = '{}'

	@apply_defaults 
	def __init__ (
				self,
				redshift_conn_id="",
				query = "",
				table = "",
				operation = "",
				*args, **kwargs):
        
        
		super(LoadDimensionOperator, self).__init__(*args, **kwargs)
		#self.aws_credentials_id = aws_credentials_id,
		self.redshift_conn_id = redshift_conn_id
		self.query = query
		self.table = table
		self.operation = operation

	def execute(self, context):
    	"""
        Loads data from staging tables into dimension tables in AWS redshift.
        PARAMS:
        redshift_conn_id = name of connection id stored in redshift for Airflow to connect
        query = string of SQL query to be used to INSERT data into dimensional tables
        table = name of destination table 
        operation = insert or delete data in destination table, 
        """


		self.log.info('Loading Dimension Tables')

		sql_redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
		dimension_query = LoadDimensionOperator.load_sql_template.format(self.query)

		if self.operation == 'insert':
			sql_redshift_hook.run(dimension_query)
		else:
			sql_redshift_hook.run(f'DELETE FROM {self.table}')	

