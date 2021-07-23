from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_create_tables import CreateTable

class CreateTablesOperator(BaseOperator):
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
        
        super(CreateTablesOperator, self).__init__(*args, **kwargs)
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
        if "event" in self.table:
            self.table = 'staging_events'
            query = CreateTable.create_staging_events_table.format(self.database)
        elif 'songs' in self.table:
            self.table = 'staging_songs' 
            query = CreateTable.create_staging_songs_table.format(self.database)
        elif ('songplays' or 'fact') in self.table:
            self.table = 'songplays'
            query = CreateTable.songplay_table_create.format(self.database)
        elif 'user' in self.table:
            query = CreateTable.user_table_create.format(self.database, self.table)
        elif 'artist' in self.table:
            query = CreateTable.artist_table_create.format(self.database, self.table)
        elif 'time' in self.table:
            query = CreateTable.time_table_create.format(self.database, self.table)
        else:
            query = CreateTable.song_table_create.format(self.database, self.table)
        
        # create the desired table
        redshift_hook.run(query)

        

        