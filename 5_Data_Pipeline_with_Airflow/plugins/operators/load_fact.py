from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries
from helpers.sql_create_tables import CreateTable

class LoadFactOperator(BaseOperator):
    """Operator for Extracting data from the stage tables, Transforming data elements
    where necessary and Loading them to the songplays fact table.

    Args:
        redshift_conn_id (str): Postgres connection name created by the user on Airflow;
        source_schema (str): Database where the staging tables are located
        target_schema (str): Database where the fact table is located
        table (str): table name (just for UX, the table will always be called songplays
            in order to not overcomplicate the creation of time table)
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 source_schema = 'public',
                 target_schema = 'public',
                 table = 'songplays',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.source_schema = source_schema
        self.target_schema = target_schema
        self.table = table

    def execute(self, context):
        self.log.info('LoadFactOperator starting')
        
        # Connects to Redshift 
        self.log.info('Connecting to Redshift')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        # Insert / append data to the fact table
        self.log.info(f'Loading data into {self.table} table')
        query = f"""INSERT INTO {self.target_schema}.{self.table}
                {SqlQueries.songplay_table_insert.format(source_schema = self.source_schema)}
        """
        redshift_hook.run(query)