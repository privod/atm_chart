import datetime as dt
import time as tm

import pandas as pd

from project.date_range import date_range
from project.pd import df_cartesian
from project.pd.datasource.orcl import DataFrameReaderOrcl
import project.atmserv.typ as typ


def calc_idle(reader, date_beg, date_end):

    df_atm_all = reader.get_atm()
    df_service = reader.get_service(date_beg, date_end)
    df_orders = reader.get_orders(date_beg, date_end)

    df_atm = df_service.merge(df_atm_all, on=['ATM_REF'])
    print(df_atm)

    # Список дат для анализа
    days = date_range(date_beg, date_end)
    df_days = pd.DataFrame({'DAY':[d for d in days]})

# --- Рсчет сервисного окна для каждого УС ------------------------------------
    df_atm_sw = df_cartesian(df_atm, df_days)

    df_atm_sw['SW_BEG'] = df_atm_sw["A_TIME_BEG"]
    df_atm_sw['SW_END'] = df_atm_sw["A_TIME_END"]

    df_atm_sw.loc[df_atm_sw.DAY.dt.weekday + 1 > df_atm_sw.A_DAYS, ['SW_BEG', 'SW_END']] = [dt.time(0), dt.time(0)]

    # TODO Необходимо учитывать режим доступности УС

    df_atm_sw.to_csv('data/df_res.csv', sep='\t')
# -----------------------------------------------------------------------------


    # for row in df_atm_sw.itertuples():


    # def test(v):
    #     return len(v)
    #
    # df_atm_sw = df_orders.merge(df_atm, on='ATM_REF', suffixes=('_ORDER', '_ATM')).apply(test)


    # def aggrfunc_test(v):
    #     return len(v)
    #
    # grp = df_res.groupby(by=['ATM_REF', 'SERIAL', 'ATM_TYPE', 'A_NUMBER_ATM', 'PRODUCER', 'MODEL', 'CITY', 'ADDR']).apply(aggrfunc_test)
    # pvt = df_res.pivot_table(index=['ATM_REF', 'SERIAL', 'ATM_TYPE', 'A_NUMBER_ATM', 'PRODUCER', 'MODEL', 'CITY', 'ADDR'], values=['REF'], aggfunc=aggrfunc_test)






    day = dt.date(2017, 4, 1)
    print(day)


    df_work_time = df_atm.apply(lambda atm, day: pd.Series(get_work_time(atm, day), index=['WBEG', 'WEND']), axis=1, args=[day])
    print(df_work_time)




