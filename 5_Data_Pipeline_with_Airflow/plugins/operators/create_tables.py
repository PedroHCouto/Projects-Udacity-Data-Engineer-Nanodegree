from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_create_tables import CreateTable

class CreateTablesOperator(BaseOperator):
    """Class for creating the staging, fact and dimensions tables that should be used in the project

    This operator aims to bring more parallelization to the DAG and should be put, in a latter step,
    in a SubDAG.

    Through this class the user gains the possibility to chose the destination database where the
    tables should be created, as well as the names of the dimension tables.

    
    Args:
       redshift_conn_id (str): Postgres connection name created by the user on Airflow; 
       target_database (str): destination database where the table should be created;
       table (str): name of the table o be created.
    """

    @apply_defaults
    def __init__(self,
            redshift_conn_id = '',
            target_database = 'public',
            table = '',
            *args, **kwargs):
        
        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.database = target_database
        self.table = table


    def execute(self, context):
        self.log.info("CreateTablesOperator starting")

        # Connects to redshift
        self.log.info("Connecting to Redshift")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)


        # check which table should be created
        self.log.info(f'Creating {self.table} if not Exists')
        if "event" in self.table:
            self.table = 'staging_events'
            query = CreateTable.create_staging_events_table.format(self.target_database)
        elif 'songs' in self.table:
            self.table = 'staging_songs' 
            query = CreateTable.create_staging_songs_table.format(self.target_database)
        elif ('songplays' or 'fact') in self.table:
            self.table = 'songplays'
            query = CreateTable.songplay_table_create.format(self.target_database)
        elif 'user' in self.table:
            query = CreateTable.user_table_create.format(self.target_database, self.table)
        elif 'artist' in self.table:
            query = CreateTable.artist_table_create.format(self.target_database, self.table)
        elif 'time' in self.table:
            query = CreateTable.time_table_create.format(self.target_database, self.table)
        else:
            query = CreateTable.song_table_create.format(self.target_database, self.table)
        
        # create the desired table
        redshift_hook.run(query)

        

        