import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Function to upload the staging tables from files in S3 to tables in a Redshift Cluster
    and then commit each one after its executions.

    Args:
        cur: cursor to execute the queries in Redshift;
        conn: connection in order to commit every query done.
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Function to run the insert queries that take data from the staging table and upload to
    the table that the query belongs. It commits every query after executing it

    Args:
        cur: cursor to execute the queries in Redshift;
        conn: connection in order to commit every query done.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Function for stablishing a connection with Readshift by importing sensible information and other 
    configurations from the file dwh.cfg.
    
    After that the function creates a cursor and pass both to the functions for uploading data to the
    staging tables and analytical tables.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()