import datetime as dt
import time as tm

import numpy as np
import pandas as pd
from pandas import Series
from pandas.core.frame import DataFrame
from progressbar import ProgressBar

import project.date_range as dr
from project.pd import df_cartesian
from project.pd.datasource.orcl import DataFrameReaderOrcl
import project.atmserv.typ as typ


def get_order_day(df_order, df_day):
    df = df_cartesian(df_order, df_day)
    return df.loc[(df.DATE_REG.dt.date <= df.DAY) & (df.DAY <= df.DATE_END.dt.date)]


def get_in_repair(df_atm_order):
    i = 0
    in_repair_list = []
    grp_atm_order = df_atm_order.groupby(['ATM_REF', 'DAY', 'SW_BEG', 'SW_END'])[['DATE_REG', 'DATE_END']]
    bar = ProgressBar(max_value=len(grp_atm_order.size())).start()
    for (atm_ref, day, sw_beg, sw_end), grp in grp_atm_order:

        service_win = (pd.datetime.combine(day, sw_beg), pd.datetime.combine(day, sw_end))

        i += 1
        in_repair_day = []
        for order in grp.itertuples():

            # if service_win is not None:
            (beg, end) = dr.inner_join([service_win, (order.DATE_REG, order.DATE_END)])
            if None not in [beg, end]:
                in_repair_day.append((beg, end))

            if i % 100 == 0:
                bar.update(i)

        in_repair_list.extend([(atm_ref, day, *service_win, *item) for item in dr.outer_join(in_repair_day) if item is not None])

    bar.finish()

    df_in_repair = pd.DataFrame(
        in_repair_list,
        columns=['ATM_REF', 'DAY', 'SW_BEG', 'SW_END', 'REPAIR_BEG', 'REPAIR_END']
    )

    df_in_repair['SERVICE_TIME'] = df_in_repair['SW_END'] - df_in_repair['SW_BEG']
    df_in_repair['REPAIR_TIME'] = df_in_repair['REPAIR_END'] - df_in_repair['REPAIR_BEG']

    return df_in_repair


def set_sw_by_service(df_sw, s_time, sw_col_name):
    # TODO подумать ка сделать процедуру более технологичной
    for time in s_time.unique():
        cond = s_time == time       # Условие выборки
        df_sw.loc[cond, sw_col_name] = pd.Timedelta(hours=time.hour, minutes=time.minute)


def calc_idle(reader, date_beg, date_end):

    df_atm_all = reader.get_atm()
    df_service = reader.get_service(date_beg, date_end)
    df_order = reader.get_orders(date_beg, date_end)

    df_atm = df_service.merge(df_atm_all, on=['ATM_REF'])   #.set_index('ATM_REF', False)

    # Список дат для анализа
    days = dr.date_list(date_beg, date_end)
    df_day = pd.DataFrame({'DAY':[d for d in days]})
    df_day['DAY_DT'] = pd.to_datetime(df_day.DAY)           # Опорное поле дней приведенных к datetime
    df_day['WEEKDAY'] = df_day.DAY_DT.dt.weekday + 1        # Опорное поле дней недели

    df_order_day = get_order_day(df_order, df_day)

# --- Рсчет сервисного окна для каждого УС ------------------------------------
    set_sw_by_service(df_atm, df_atm.A_TIME_BEG, 'TD_SW_BEG')   # Timedelta от начала суток до открытия сервисного окна
    set_sw_by_service(df_atm, df_atm.A_TIME_END, 'TD_SW_END')   # Timedelta от начала суток до закрытия сервисного окна

    df_atm_weekday = df_cartesian(df_atm, pd.DataFrame({'WEEKDAY': [1, 2, 3, 4, 5, 6, 7]})) #.set_index(['ATM_REF', 'WEEKDAY'], False)
    cond = df_atm_weekday.WEEKDAY > df_atm_weekday.A_DAYS       # Условие выборки: Выходные дни для УС не обслуживающихся выходной по условиям обслуживания
    df_atm_weekday.loc[cond, 'TD_SW_BEG'] = pd.to_timedelta(0)
    df_atm_weekday.loc[cond, 'TD_SW_END'] = pd.to_timedelta(0)

    df_atm_sw = pd.merge(df_atm_weekday, df_day, 'outer', on='WEEKDAY').set_index(['ATM_REF', 'DAY'], False).sort_index()

    df_atm_sw['SW_BEG'] = df_atm_sw.DAY_DT + df_atm_sw.TD_SW_BEG
    df_atm_sw['SW_END'] = df_atm_sw.DAY_DT + df_atm_sw.TD_SW_END

    # TODO Необходимо учитывать режим доступности УС

    # hours = (df_atm_sw.MON_END / 100).astype(int).astype(str)
    # minuets = (df_atm_sw.MON_END % 100).astype(str)

    print(df_atm_sw.loc[df_atm_sw.MON_END != 0, 'MON_END'])

    print(pd.to_datetime(df_atm_sw.loc[df_atm_sw.MON_END != 0, 'MON_END'].astype(str), format='%H%M'))
    # print(pd.to_datetime(df_atm_sw.MON_BEG, format='%H%M'))
    # df_atm_sw[df_atm_sw.df_atm_sw.DAY_DT.dt.weekday == 0, 'SW_BEG'] =


    print(df_atm_sw)
    df_atm_sw.to_csv('data/df_atm_sw.csv', sep='\t')
    return
# -----------------------------------------------------------------------------

    df_atm_order = pd.merge(df_atm_sw, df_order_day, on=['ATM_REF', 'DAY']).set_index(['ATM_REF', 'DAY'], False)
    df_atm_order.to_csv('data/df_atm_order.csv', sep='\t')

    df_in_repair = get_in_repair(df_atm_order)
    # df_in_repair.to_pickle('data/df_in_repair.pd', compression='xz')
    df_in_repair = pd.read_pickle('data/df_in_repair.pd', compression='xz')
    df_in_repair = df_in_repair.set_index(['ATM_REF', 'DAY'], False)
    print(df_in_repair.head())

    # print(df_in_repair.info())
    # print(df_atm_sw.info())
    # print(df_atm_sw[['ATM_REF', 'DAY', 'SERIAL', 'CITY', 'ADDR', 'MODEL']])

    df_idle = pd.merge(
        df_atm_sw[['ATM_REF', 'DAY', 'SERIAL', 'CITY', 'ADDR', 'MODEL']],
        df_in_repair,
        how='left',
        on=['ATM_REF', 'DAY']
    )

    df_idle['AVAIL'] = 1 - (df_idle['REPAIR_TIME'] / df_idle['SERVICE_TIME'])
    print(df_idle)
    df_idle.to_csv('data/df_idle.csv', sep='\t')


