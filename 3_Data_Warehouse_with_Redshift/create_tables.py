import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Function to drop all tables present in the list "drop_table_queries" 
    in the sql_queries.py file.
    
    Args:
        cur: cursor to execute the queries in Redshift;
        conn: connection in order to commit every query done.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Function to create tables by executing all the queries present in the list "create_table_queries" 
    in the sql_queries.py file.
    
    Args:
        cur: cursor to execute the queries in Redshift;
        conn: connection in order to commit every query done.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Function for stablishing a connection with Readshift by importing sensible information and other 
    configurations from the file dwh.cfg.
    
    After that the function creates a cursor and pass both to the functions for dropping existing tables
    and creating new ones as specified in the sql_queries.py file.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()