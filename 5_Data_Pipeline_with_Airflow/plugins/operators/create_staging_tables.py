from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_create_tables import CreateTable

class CreateStagingTablesOperator(BaseOperator):
    """

    
    Args:
        BaseOperator ([type]): [description]
    """

    @apply_defaults
    def __init__(self,
            redshift_conn_id = '',
            database = 'public',
            table = '',
            *args, **kwargs):
        
        super(CreateStagingTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.database = database
        self.table = table


    def execute(self, context):
        self.log.info("Starting the create_staging_tables operator")

        # Connects to redshift
        self.log.info("Connecting to Redshift")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)


        # check which table should be created
        self.log.info(f'Creating {self.table} if not Exists')
        if "events" in self.table:
            table = 'staging_events'
            create_table_query = CreateTable.create_staging_events_table
        else:
            table = 'staging_songs' 
            create_table_query = CreateTable.create_staging_songs_table
        redshift_hook.run(create_table_query.format(self.database))

        self.log.info('Deleting any data present in the table')
        redshift_hook.run(f'DELETE FROM {self.database}.{table}')