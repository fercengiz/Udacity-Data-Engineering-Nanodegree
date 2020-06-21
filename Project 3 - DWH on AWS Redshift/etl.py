import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from files stored in S3 to the staging tables using the queries declared on the sql_queries script
    Args:
        cur (`psycopg2.extensions.cursor`): Cusrsor for db connection
        conn (`psycopg2.extensions.connection`): connection for database
    Returns:
        None
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)



def insert_tables(cur, conn):
    """Select and Transform data from staging tables into the dimensional tables using the queries declared on the sql_queries script
    Args:
        cur (`psycopg2.extensions.cursor`): Cusrsor for db connection
        conn (`psycopg2.extensions.connection`): connection for database
    Returns:
        None
    """
    for query in insert_table_queries:
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

       
    load_staging_tables(cur, conn)
    print("Staging is completed")
    
    insert_tables(cur, conn)
    print("Loading is completed")
    
    
    try:
        conn.close()
    except Exception as e:
        print(e)
        
    print("ETL is completed. All tables in sql_queries.py have been inserted with data. (etl.py is completed!)")


if __name__ == "__main__":
    main()