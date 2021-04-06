import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from files stored in S3 to the staging tables using the queries on the sql_queries script
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Select and transform data from staging tables into the dimensional tables using the queries on the sql_queries script
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Extract songs metadata and user activity data from S3, transform it using a staging table, and load it into dimensional tables for analysis
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('1. Connected')
    cur = conn.cursor()
    print('2. Cursor Created')
    
    load_staging_tables(cur, conn)
    print('3. Staging Loaded')
    insert_tables(cur, conn)
    print('4. Inserted into tables')

    conn.close()
    print('5. Connection Closed')


if __name__ == "__main__":
    main()