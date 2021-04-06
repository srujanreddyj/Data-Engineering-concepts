import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Delete tables if pre-existing and be able to create them from scratch
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging tables and dimensional tables mentioned on sql_queries script
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Set up the database tables, create tables with the appropriate columns and constricts
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('1. Connected')
    
    cur = conn.cursor()
    print('2. Cursor Created')

    drop_tables(cur, conn)
    print('3. Tables Dropped if already exists')
    
    create_tables(cur, conn)
    print('4. Tables Created')

    conn.close()
    print('5. Connection Closed')


if __name__ == "__main__":
    main()