import pandas as pd
from typing import Callable, Any
import psycopg2
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import time
import calendar
import numpy as np
from sqlalchemy import create_engine


def with_retry_on_connection_error(fn: Callable[..., Any], retries: int = 5, delay: float = 3.0) -> Callable:
    def wrapped(*args, **kwargs):
        for attempt in range(1, retries + 1):
            try:
                return fn(*args, **kwargs)
            except psycopg2.OperationalError as e:
                if attempt == retries:
                    raise
                print(f"[Retry] Ошибка подключения: {e}. Повтор через {delay} сек (попытка {attempt}/{retries})")
                time.sleep(delay)

    return wrapped


def safe_parse_date(date_str):
    """Пытается создать дату, если день превышает допустимое — сдвигает на последний день месяца."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        try:
            year, month, day = map(int, date_str.split('-'))
            last_day = calendar.monthrange(year, month)[1]
            return datetime(year, month, last_day)
        except Exception as e:
            print(f"Ошибка при корректировке даты: {e}")
            raise


def query_get_data_inplace(day_start, day_end):
    query_get_data_invoice = f"""
            SELECT
                i.id as order_id,
                i.amount,
                i.payer_id,
                i.created_at,
                i.status_id,
                i.engine_id,
                i.finished_at,
                lcl.system_name as currency,
                extra -> 'cardDetails' ->> 'pan' AS pan
            FROM orders.invoice i
                JOIN lists.invoice_order_type_list iotl ON i.order_type_id = iotl.id
                JOIN lists.currency_list lcl on lcl.id = i.currency_id
            WHERE i.created_at >= '{day_start}' AND i.created_at <= '{day_end}'
                AND iotl.system_name ILIKE '%H2H%'
            """
    return query_get_data_invoice


@with_retry_on_connection_error
def create_conn_dwh():
    connection = psycopg2.connect(
        dbname='postgres',
        user='ste',
        password='ILzAYQ72aEe9',
        host='primarydwhcsd.aerxd.tech',
        port=6432
    )
    return connection


def convert_dtypes(df):
    dtype_dict = {
        'date': 'datetime64[ns]',
        'pan': 'object',
        'currency': 'object',
        'orders_count': 'int64',
        'success_orders_count': 'int64',
        'amount_sum': 'float64',
        'success_amount_sum': 'float64',
        'reject_amount_sum': 'float64',
        'avg_close_time': 'float64',
        'order_type': 'object'
    }
    for col, dtype in dtype_dict.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                print(f"Не удалось привести столбец '{col}' к типу {dtype}: {e}")
    return df


def run_query_dwh(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


def query_get_data_payout(day_start, day_end, clients_ids: tuple):
    query = f"""
        SELECT
            i.id,
            i.order_id,
            i.amount,
            i.created_at,
            i.client_id,
            i.status_id,
            i.finished_at,
            lcl.system_name as currency,
            w.recipient AS pan
        FROM orders.withdraw_engine i
            JOIN lists.currency_list lcl on lcl.id = i.currency_id
            left join orders.withdraw w on w.id = i.order_id 
        WHERE i.created_at >= '{day_start}' and i.created_at <= '{day_end}' and i.client_id in {clients_ids}
    """
    return query


@with_retry_on_connection_error
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


def get_invoice_data_by_days(start_date: str, end_date: str, type_order: str):
    try:
        start = safe_parse_date(start_date)
        end = safe_parse_date(end_date)
    except Exception as e:
        print(e)
        return pd.DataFrame()
    if start > end:
        return pd.DataFrame()

    final_df = pd.DataFrame()
    current_date = start
    day_counter = 1
    conn = create_connection()

    try:
        while current_date <= end:
            day_start = current_date.strftime('%Y-%m-%d 00:00:00')
            day_end = current_date.strftime('%Y-%m-%d 23:59:59')
            print(f"День {day_counter}: {current_date.strftime('%Y-%m-%d')}")
            print(type_order)
            if type_order == 'invoice':
                query_get_data = query_get_data_inplace(day_start, day_end)
            else:
                query_get_data = query_get_data_payout(day_start, day_end, clients_ids=tuple([3960, 3949]))
            try:
                daily_data = run_query_with_conn(conn, query_get_data)
                if not daily_data.empty:
                    daily_data['processing_date'] = current_date.strftime('%Y-%m-%d')
                    final_df = pd.concat([final_df, daily_data], ignore_index=True)
                    print(f"Размер final_df - {len(final_df)}")
                    print(f"Получено {len(daily_data)} записей")
                else:
                    print(f"Нет данных")
            except Exception as e:
                print(f"Ошибка при получении данных за {current_date.strftime('%Y-%m-%d')}: {e}")

            current_date += timedelta(days=1)
            day_counter += 1
            time.sleep(0.5)
    finally:
        conn.close()  # Закрываем соединение в конце
    final_df.to_csv('ddd.csv', index=False)
    final_df = final_df.rename(columns={'display_name': 'engine'})
    return final_df


def agg_data(date_start: str, date_end: str, order_type: str) -> pd.DataFrame:
    print('launch')
    data = get_invoice_data_by_days(date_start, date_end, order_type)

    data['created_at'] = pd.to_datetime(data['created_at'], utc=True, errors='coerce')
    data['finished_at'] = pd.to_datetime(data['finished_at'], utc=True, errors='coerce')

    try:
        data['close_diff'] = (data['finished_at'] - data['created_at']).dt.total_seconds().astype(float)
    except pd.errors.IntCastingNaNError:
        data['close_diff'] = np.nan

    if order_type == 'invoice':
        success_status_id = 2
    else:
        success_status_id = 4

    data['success_amount'] = data['amount'].where(data['status_id'] == success_status_id, 0)
    data['reject_amount'] = data['amount'].where(data['status_id'] != success_status_id, 0)
    data_group = data.copy()
    print(data_group.shape, 'qaz')
    for col in data_group.select_dtypes(include=['datetimetz']).columns:
        data_group[col] = data_group[col].dt.tz_localize(None)

    data_group['date'] = data_group['created_at'].dt.to_period('D')
    print(data_group)
    print(data_group.columns)
    data_group.to_csv('data_group.csv', index=False)
    data_group = (
        data_group
        .groupby(
            ['date', 'pan', 'currency'])
        .agg(
            orders_count=('order_id', 'count'),
            success_orders_count=('status_id', lambda x: (x == success_status_id).sum()),
            amount_sum=('amount', 'sum'),
            success_amount_sum=('success_amount', 'sum'),
            reject_amount_sum=('reject_amount', 'sum'),
            avg_close_time=('close_diff', 'mean')
        )
        .reset_index()
    )
    print(data_group.shape, 'qaz')
    data_group['date'] = data_group['date'].astype(str)
    return data_group


def align_to_monday(date_str):
    """
    Приводит дату к предыдущему понедельнику (если уже понедельник — не меняет).
    """
    date = pd.to_datetime(date_str)
    return (date - pd.Timedelta(days=date.weekday())).date()


def execute_functions_mode(mode, order_type):
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/postgres'.format(
        user='ste',
        password='ILzAYQ72aEe9',
        host='primarydwhcsd.aerxd.tech',
        port=6432)
    )

    if mode == 'update':
        conn = create_conn_dwh()
        date_end = \
            run_query_dwh(f"""SELECT max("date") as max_date FROM cascade.cards_info_sg""", conn)['max_date'].iloc[0]

        date_start = (date_end - relativedelta(months=1)).strftime('%Y-%m-%d')
        date_end = date_end.strftime('%Y-%m-%d')
        print(date_end, date_start)
        data_invoice = agg_data(date_start, date_end, order_type)
        query_get_from_dwh = f"""SELECT * FROM cascade.cards_info_sg
                                 WHERE "date" >= '{date_start}' AND "date" <= '{date_end}'
                             """
        conn = create_conn_dwh()
        data_from_dwh = run_query_dwh(query_get_from_dwh, conn)

        print(data_from_dwh.shape)
        print(data_invoice.shape)

        data_invoice = convert_dtypes(data_invoice)
        data_from_dwh = convert_dtypes(data_from_dwh)

        data_invoice = data_invoice.round(2)
        data_from_dwh = data_from_dwh.round(2)

        data_invoice = data_invoice.merge(data_from_dwh,
                                          on=['date', 'pan', 'currency'],
                                          how='inner', suffixes=('_new', '_old'))
        data_invoice.to_csv('test_inv.csv', index=False)
        data_invoice = data_invoice[(data_invoice['amount_sum_new'] != data_invoice['amount_sum_old']) |
                                    (data_invoice['success_orders_count_new'] != data_invoice[
                                        'success_orders_count_old']) |
                                    (data_invoice['success_amount_sum_new'] != data_invoice['success_amount_sum_old']) |
                                    (data_invoice['reject_amount_sum_new'] != data_invoice['reject_amount_sum_old']) |
                                    (data_invoice['avg_close_time_new'] != data_invoice['avg_close_time_old'])
                                    ]

        data_invoice = data_invoice.drop(columns=[col for col in data_invoice.columns if col.endswith('_old')])
        data_invoice = data_invoice.rename(
            columns={col: col.replace('_new', '') for col in data_invoice.columns if col.endswith('_new')})

        conn.close()
        conn = create_conn_dwh()
        print(data_invoice)
        with conn.cursor() as cursor:
            for index, row in data_invoice.iterrows():
                update_query = """
                UPDATE cascade.cards_info_sg
                SET amount_sum = %s,
                    success_orders_count = %s,
                    success_amount_sum = %s,
                    reject_amount_sum = %s,
                    avg_close_time = %s  
                WHERE "date" = %s AND
                      pan = %s AND
                      currency = %s AND
                      order_type = %s      
                """
                cursor.execute(update_query, (
                    row['amount_sum'],
                    row['success_orders_count'],
                    row['success_amount_sum'],
                    row['reject_amount_sum'],
                    row['avg_close_time'],
                    row['date'],
                    row['pan'],
                    row['currency'],
                    order_type
                ))
            conn.commit()
            conn.close()
        print(data_invoice.shape)
        return data_invoice
    else:
        now = datetime.now()
        date_start = now.date()
        date_end = now.date()

        date_start = date_start.strftime('%Y-%m-%d')
        date_end = date_end.strftime('%Y-%m-%d')
        date_start = '2025-08-10'
        date_end = '2025-08-11'
        print(date_start, date_end)
        data_invoice = agg_data(date_start, date_end, order_type)
        data_invoice['order_type'] = order_type
    print(data_invoice.dtypes)
    print(data_invoice.shape)
    if not data_invoice.empty:
        data_invoice = data_invoice.round(2)
        data_invoice.to_csv('result.csv', index=False)

        conn = create_conn_dwh()
        with conn.cursor() as cur:
            delete_query = """
                        DELETE FROM cascade.cards_info_sg
                        WHERE "date" >= %s AND
                              "date" <= %s AND
                              "order_type" = %s 
                        
                    """
            cur.execute(delete_query, (date_start, date_end, order_type))
        conn.commit()

        data_invoice.to_sql(
            schema='cascade',
            name='cards_info_sg',
            if_exists='append',
            con=engine,
            index=False
        )
    return data_invoice


def main():
    execute_functions_mode(mode='upload', order_type='payout')
    print('Отработал: mode - upload, order_type - payout')

    execute_functions_mode(mode='update', order_type='payout')
    print('Отработал: mode - update, order_type - payout')
    #
    execute_functions_mode(mode='upload', order_type='invoice')
    print('Отработал: mode - upload, order_type - invoice')

    execute_functions_mode(mode='update', order_type='invoice')
    print('Отработал: mode - update, order_type - invoice')


if __name__ == '__main__':
    main()
