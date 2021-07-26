from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from plugins.helpers.sql_queries import SqlQueries
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """Operator for uploading from the staging and fact tables into the dimension tables.

    Here the user have the chance to specify the source database, target database and table
    as well as the mode the operator should use for inserting data.

    Args:
        redshift_conn_id (str): Postgres connection name created by the user on Airflow;
        source_database (str): Database where the staging tables are located;
        target_database (str): Database where the dimension table is located;
        table (str): dimensional table which will receive the data
        append_mode (Bool): True if the user desire to append the new data to the existing rows
        primary_key (str): name of primary key for comparingson in order to avoid dupplicated 
            elements in append mode. Only necessary if append_mode = True.
    """


    @apply_defaults
    def __init__(self,
                 redshit_conn_id = 'redshift',
                 source_database = 'public',
                 target_database = 'public',
                 table = '',
                 append_mode = True,
                 primary_key = None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshit_conn_id
        self.source_database = source_database
        self.target_database = target_database
        self.table = table
        self.append_mode = append_mode
        self.primary_key = primary_key

    def execute(self, context):
        self.log.info('LoadDimensionOperator starting')

        # Connects to Redshift 
        self.log.info('Creating a connection to Redshift')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        # Check the mode and processing it as desired                
        if self.append_mode == False:
            self.log.info(f'Cleaning {self.table} befeore insterting new data')
            redshift_hook.run(f'DELETE FROM {self.target_database}.{self.table}')

        # Check which table should be targeted in the ETL process
        if 'user' in self.table:
            query = SqlQueries.user_table_insert.format(source_database = self.source_database)
        elif 'song' in self.table: 
            query = SqlQueries.song_table_insert.format(source_database = self.source_database)
        elif 'artist' in self.table:
            query = SqlQueries.artist_table_insert.format(source_database = self.source_database)
        else:
            query = SqlQueries.time_table_insert.format(source_database = self.source_database)
        query = f"""INSERT INTO {self.target_database}.{self.table}
            {query}
        """

        if self.append_mode:
            query = """
                CREATE TEMP TABLE {0}.stage_{1} (LIKE {0}.{1}); 
                
                INSERT INTO {0}.stage_{1}
                {2};
                
                DELETE FROM {0}.{1}
                USING {0}.stage_{1}
                WHERE {0}.{1}.{3} = {0}.stage_{1}.{3};
                
                INSERT INTO {0}.{1}
                SELECT * FROM {0}.stage_{1};
            """.format(self.target_database, 
                       self.table, 
                       query,
                       self.primary_key)

        else:
            query = f"""
                INSERT INTO {self.target_database}.{self.table}
                {query}
            """
            # Deleting any existing data on the table
            self.log.info("Truncating data from destination Redshift table")
            redshift_hook.run(f"TRUNCATE TABLE {self.target_database}.{self.table};").format(self.target_database, self.table)

        self.info.log(f'Inserting data into {self.target_database}.{self.table}')
        redshift_hook.run(query)
