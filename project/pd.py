import pandas as pd
import pandas.io.sql as psql
import cx_Oracle
import os

os.environ['NLS_LANG'] = 'American_America.AL32UTF8'


def get_connect(user='privod_ust_atm_new', password='121', dns='orcl'):
    connect = None

    try:
        connect = cx_Oracle.connect(user, password, dns)
    except cx_Oracle.DatabaseError as info:
        print("DB logon  error:", info)  # TODO Зменить на логирование
        exit(0)

    return connect


def get_df_test(connect, date):
    sql_text = """
select * from r_atm a
  inner join r_atm_arc aa on a.a_atm_arc = aa.a_atm_arc
  inner join r_service_arc sa on aa.ref = sa.atm_ref
  inner join r_service s on s.a_serv_arc = sa.a_serv_arc
  inner join r_order o on o.service_type in (1, 2) 
    and o.atm_ref = a.ref 
    and :d between trunc(o.date_reg/1000000) and  trunc(o.date_end/1000000)
where 1 = 1
  and sa.type in (1, 2)
  and :d between sa.a_start and a_finish
"""

    return psql.read_sql_query(sql=sql_text, con=connect, params={'d': date})


def get_df_atm(connect, beg, end):
    sql_text = """
select aa.* from r_atm a
  inner join r_atm_arc aa on a.a_atm_arc = aa.a_atm_arc
  inner join r_service_arc sa on aa.ref = sa.atm_ref
  inner join r_service s on s.a_serv_arc = sa.a_serv_arc
where 1 = 1
  and sa.type in (1, 2)
  and :beg < a_finish
  and :end > sa.a_start
"""
    return psql.read_sql_query(sql=sql_text, con=connect, params={'beg': beg, 'end': end})


def get_df_orders(connect, beg, end):
    sql_text = """
select * from r_order o
where 1 = 1
  and o.service_type in (1, 2) 
  and :beg < trunc(o.date_end/1000000)
  and :end > trunc(o.date_reg/1000000)
"""
    return psql.read_sql_query(sql=sql_text, con=connect, params={'beg': beg, 'end': end})


# df = pd.read_csv('data/kp_all_movies.csv') #скачиваем подготовленные данные

conn = get_connect()

date_beg = '20170501'
date_end = '20170531'

df = get_df_test(conn, date_beg)
df_atm = get_df_atm(conn, date_beg, date_end)
df_orders = get_df_orders(conn, date_beg, date_end)


# print(df_atm.head())
# print(df_orders.head())
#
# print(df_atm.SERIAL)
#
# atm = df_atm[df_atm.SERIAL == '13-44006153']
# print(atm)

# print(df_atm.info())
# print(df_orders.info())

res = df_orders.merge(df_atm, left_on='ATM_REF', right_on='REF')

print(res)


