import pandas as pd
import psycopg2


def create_conn_dwh():
    connection = psycopg2.connect(
        dbname='postgres',
        user='ste',
        password='ILzAYQ72aEe9',
        host='primarydwhcsd.aerxd.tech',
        port=6432
    )
    return connection


def create_connection():
    return psycopg2.connect(
        dbname='csd_bi',
        user='datalens_utl',
        password='mQnXQaHP6zkOaFdTLRVLx40gT4',
        host='138.68.88.175',
        port=5432
    )


def run_query_with_conn(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
    return df


def run_query_dwh(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


new_df = pd.read_csv('granularity_new.csv')
print(tuple(new_df['payer_id'].unique()))

query = f"""
            select COUNT(id), payer_id from orders.invoice i 
            where created_at > '01-04-2025' and payer_id in {tuple(new_df['payer_id'].unique())}
            group by payer_id
        """

df = run_query_with_conn(create_connection(), query)

print(df)
