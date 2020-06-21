import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Load data from files stored in S3 to the staging tables using the queries declared on the sql_queries script
    Args:
        cur (`psycopg2.extensions.cursor`): Cusrsor for db connection
        conn (`psycopg2.extensions.connection`): connection for database
    Returns:
        None
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def create_tables(cur, conn):
    """Load data from files stored in S3 to the staging tables using the queries declared on the sql_queries script
    Args:
        cur (`psycopg2.extensions.cursor`): Cusrsor for db connection
        conn (`psycopg2.extensions.connection`): connection for database
    Returns:
        None
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def main():
    
    try:
        config = configparser.ConfigParser()
        config.read('dwh.cfg')
    except Exception as e:
        print(e)

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except Exception as e:
        print(e)

    drop_tables(cur, conn)
    print("Dropping tables is completed (if existed)")
    create_tables(cur, conn)
    print("Creating tables is completed")

    try:
        conn.close()
    except Exception as e:
        print(e)
    
    print("All tables in sql_queries.py have been dropped (if existed) and created. (create_tables.py is completed!)")

if __name__ == "__main__":
    main()