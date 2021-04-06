from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    load_sql = """
        INSERT INTO {} ({});
    """

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 query="",
                 insert_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query
        self.insert_mode = insert_mode

    def execute(self, context):
        """
        Loads data from staging tables into dimension tables in AWS redshift.

        PARAMS:
        conn_id = name of connection id stored in redshift for Airflow to connect
        query = string of SQL query to be used to INSERT data into dimensional tables
        table = name of destination table 
        insert_mode = truncate-insert = delete data in destination table, 
        """
        
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        if self.insert_mode == ' truncate-insert':
            redshift.run("DELETE FROM {}".format(self.table))
            redshift.run(LoadDimensionOperator.load_sql.format(self.table, self.query))
        else:
            redshift.run(LoadDimensionOperator.load_sql.format(self.table, self.query))