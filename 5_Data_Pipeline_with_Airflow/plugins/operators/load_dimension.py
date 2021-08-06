from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """Operator for uploading from the staging and fact tables into the dimension tables.

    Here the user have the chance to specify the source database, target database and table
    as well as the mode the operator should use for inserting data.

    Args:
        redshift_conn_id (str): Postgres connection name created by the user on Airflow;
        source_schema (str): Database where the staging tables are located;
        target_schema (str): Database where the dimension table is located;
        table (str): dimensional table which will receive the data
        append_mode (Bool): True if the user desire to append the new data to the existing rows
        primary_key (str): name of primary key for comparingson in order to avoid dupplicated 
            elements in append mode. Only necessary if append_mode = True.
    """


    @apply_defaults
    def __init__(self,
                 redshit_conn_id = 'redshift',
                 source_schema = 'public',
                 target_schema = 'public',
                 table = '',
                 append_mode = False,
                 primary_key = None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshit_conn_id
        self.source_schema = source_schema
        self.target_schema = target_schema
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
            redshift_hook.run(f'DELETE FROM {self.target_schema}.{self.table}')

        # Check which table should be targeted in the ETL process
        if 'user' in self.table:
            query = SqlQueries.user_table_insert.format(source_schema = self.source_schema)
            source_table = f'{self.source_schema}.staging_events'
        elif 'song' in self.table: 
            query = SqlQueries.song_table_insert.format(source_schema = self.source_schema)
            source_table = f'{self.source_schema}.staging_songs'
        elif 'artist' in self.table:
            query = SqlQueries.artist_table_insert.format(source_schema = self.source_schema)
            source_table = f'{self.source_schema}.staging_songs'
        else:
            query = SqlQueries.time_table_insert.format(source_schema = self.source_schema)
            source_table = f'{self.source_schema}.songplays'


        if self.append_mode:
            # Sources:
            # - https://docs.aws.amazon.com/redshift/latest/dg/merge-replacing-existing-rows.html
            # - http://www.silota.com/blog/amazon-redshift-upsert-support-staging-table-replace-rows/
            query = f"""
                DELETE FROM {self.target_schema}.{self.table}
                USING {source_table}
                WHERE {self.target_schema}.{self.table}.{self.primary_key} = {source_table}.{self.primary_key};

                INSERT INTO {self.target_schema}.{self.table} {query};
            """
            

        else:
            # Deleting any existing data on the table
            self.log.info("Truncating data from destination Redshift table")
            redshift_hook.run(f"TRUNCATE TABLE {self.target_schema}.{self.table};")
            
            query = f"""INSERT INTO {self.target_schema}.{self.table}
                    {query}
                    """

        self.log.info(f'Inserting data into {self.target_schema}.{self.table}')
        redshift_hook.run(query)
