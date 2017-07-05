import datetime as dt
import time as tm
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


def get_df_service(connect, beg, end):
    sql_text = """
select sa.* from r_service s
  inner join r_service_arc sa on s.a_serv_arc = sa.a_serv_arc
where 1 = 1
  and sa.type in (1, 2)
  and :beg < sa.a_finish
  and :end > sa.a_start
"""
    return psql.read_sql_query(sql=sql_text, con=connect, params={'beg': beg, 'end': end})


def get_df_atm(connect, beg, end):
    sql_text = """
select aa.ref atm_ref, aa.serial, aa.a_number, aa.type, aa.producer_name producer,
  aa.model_name model, aa.city, aa.addr, aa.location, aa.bank_ref,
  sa.ref service_ref, sa.contract_ref, sa.type service_type, sa.a_start, sa.a_finish,
  sa.a_recovery, sa.a_days, sa.a_time_beg, sa.a_time_end, sa.a_price, sa.a_zone
from r_atm a       
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
    return psql.read_sql_query(
        sql=sql_text, con=connect, params={'beg': beg, 'end': end}
        # , parse_dates={'DATE_REG': '%Y%m%d%H%M%S'}
    )


def date_range(start, stop, step=dt.timedelta(days=1), inclusive=True):
    # inclusive=False to behave like range by default
    if step.days > 0:
        while start < stop:
            yield start
            start = start + step
    elif step.days < 0:
        while start > stop:
            yield start
            start = start - step
    if inclusive and start == stop:
        yield start

# df = pd.read_csv('data/kp_all_movies.csv') #скачиваем подготовленные данные

conn = get_connect()

GP_DATE_FORMAT = '%Y%m%d'
GP_TIME_FORMAT = '%H%M%S'
GP_DATETIME_FORMAT = '%Y%m%d%H%M%S'

date_beg = dt.datetime(2017, 5, 1)
date_end = dt.datetime(2017, 5, 31)

gp_date_beg = date_beg.strftime(GP_DATE_FORMAT)
gp_date_end = date_end.strftime(GP_DATE_FORMAT)

df = get_df_test(conn, gp_date_beg)
df_atm = get_df_atm(conn, gp_date_beg, gp_date_end)
df_orders = get_df_orders(conn, gp_date_beg, gp_date_end)

df_orders.DATE_REG = pd.to_datetime(df_orders.DATE_REG, format=GP_DATETIME_FORMAT)
df_orders.DATE_END = pd.to_datetime(df_orders.DATE_END, format=GP_DATETIME_FORMAT)

days = date_range(date_beg, date_end)
df_days = pd.DataFrame({'DAY':[d for d in days]})
# print(df_days)

# print(df_atm.head())
# print(df_orders.head())

# print(df_atm.SERIAL)

# atm = df_atm[df_atm.SERIAL == '13-44006153']
# print(atm)

# print(df_atm.info())
# print(df_orders.info())

df_res = df_orders.merge(df_atm, on='ATM_REF', suffixes=('_ORDER', '_ATM'))
# print(df_res)
df_res.to_csv('data/df_res.csv', sep='\t')
# print(df_res.describe())


# pvt = df_atm.pivot_table(index='MODEL', columns=['SERVICE_TYPE'], values='SERIAL', aggfunc='count')
# print(pvt)


def aggrfunc_test(v):
    # print('aggrfunc_test:')
    # print(v)

    return len(v)


grp = df_res.groupby(by=['ATM_REF', 'SERIAL', 'ATM_TYPE', 'A_NUMBER_ATM', 'PRODUCER', 'MODEL', 'CITY', 'ADDR']).apply(aggrfunc_test)
# print(grp)

pvt = df_res.pivot_table(index=['ATM_REF', 'SERIAL', 'ATM_TYPE', 'A_NUMBER_ATM', 'PRODUCER', 'MODEL', 'CITY', 'ADDR'], values=['REF'], aggfunc=aggrfunc_test)
# print(pvt)

# df_res = pd.crosstab(df_res, df_days)
# print(df_res)


# def get_work_time(beg, end):
#     return pd.Series([beg, end], index=['WBEG', 'WEND'])


def get_work_time(atm, day):
    w = day.weekday()

    if w > atm.A_DAYS:
        return dt.time(0), dt.time(0)

    def get_time(t):
        struct = tm.strptime(str(t), GP_TIME_FORMAT)
        return dt.time(struct.tm_hour, struct.tm_min)

    beg = get_time(atm.A_TIME_BEG)
    end = get_time(atm.A_TIME_END)

    # TODO Необходимо учитывать режим доступности УС

    return beg, end


day = dt.date(2017, 4, 1)
print(day)


df_work_time = df_atm.apply(lambda atm, day: pd.Series(get_work_time(atm, day), index=['WBEG', 'WEND']), axis=1, args=[day])
print(df_work_time)


# df_atm[] = df

# for day in days:
#     df_atm[day] = df

