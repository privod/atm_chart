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
    return df.loc[(df.DATE_REG <= df.DAY) & (df.DAY <= df.DATE_END)]


def set_group_func(df, series, col_name, func):

    for item in series.unique():
        cond = series == item       # Булев массив для каждого уникального значения
        df.loc[cond, col_name] = func(item)


def get_in_repair(df_atm_order):
    i = 0
    in_repair_list = []
    grp_atm_order = df_atm_order.groupby(['ATM_REF', 'DAY', 'SW_BEG', 'SW_END'])[['DATE_REG', 'DATE_END']]
    bar = ProgressBar(max_value=len(grp_atm_order.size())).start()
    for (atm_ref, day, sw_beg, sw_end), grp in grp_atm_order:

        i += 1
        in_repair_day = []
        for order in grp.itertuples():

            (beg, end) = dr.inner_join([(sw_beg, sw_end), (order.DATE_REG, order.DATE_END)])
            if None not in [beg, end]:
                in_repair_day.append((beg, end))

            if i % 100 == 0:
                bar.update(i)

        in_repair_list.extend([(atm_ref, day, sw_beg, sw_end, *item) for item in dr.outer_join(in_repair_day) if item is not None])

    bar.finish()

    df_in_repair = pd.DataFrame(
        in_repair_list,
        columns=['ATM_REF', 'DAY', 'SW_BEG', 'SW_END', 'REPAIR_BEG', 'REPAIR_END']
    )

    # df_in_repair['REPAIR_TIME'] = df_in_repair['REPAIR_END'] - df_in_repair['REPAIR_BEG']

    return df_in_repair


def get_service_window(df_atm, df_day):

    def time_to_td(time):
        return pd.Timedelta(hours=time.hour, minutes=time.minute)

    set_group_func(df_atm, df_atm.A_TIME_BEG, 'TD_SERVICE_BEG', time_to_td)  # Timedelta от начала суток до начала обслуживания
    set_group_func(df_atm, df_atm.A_TIME_END, 'TD_SERVICE_END', time_to_td)  # Timedelta от начала суток до окончания обслуживания

    # Рачет режима обслуживания по условиям дней недели
    df_atm_wd = df_cartesian(df_atm, pd.DataFrame({'WEEKDAY': [1, 2, 3, 4, 5, 6, 7]})) #.set_index(['ATM_REF', 'WEEKDAY'], False)
    cond = df_atm_wd.WEEKDAY > df_atm_wd.A_DAYS
    df_atm_wd.loc[cond, 'TD_SERVICE_BEG'] = pd.to_timedelta(0)
    df_atm_wd.loc[cond, 'TD_SERVICE_END'] = pd.to_timedelta(0)

    # Режим доступности УС сводится к двум полям (AVAIL_BEG и AVAIL_END) в разрезе недель
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 1, 'TIME_AVAIL_BEG'] = df_atm_wd.MON_BEG
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 1, 'TIME_AVAIL_END'] = df_atm_wd.MON_END
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 2, 'TIME_AVAIL_BEG'] = df_atm_wd.TUE_BEG
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 2, 'TIME_AVAIL_END'] = df_atm_wd.TUE_END
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 3, 'TIME_AVAIL_BEG'] = df_atm_wd.WED_BEG
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 3, 'TIME_AVAIL_END'] = df_atm_wd.WED_END
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 4, 'TIME_AVAIL_BEG'] = df_atm_wd.THU_BEG
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 4, 'TIME_AVAIL_END'] = df_atm_wd.THU_END
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 5, 'TIME_AVAIL_BEG'] = df_atm_wd.FRI_BEG
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 5, 'TIME_AVAIL_END'] = df_atm_wd.FRI_END
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 6, 'TIME_AVAIL_BEG'] = df_atm_wd.SAT_BEG
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 6, 'TIME_AVAIL_END'] = df_atm_wd.SAT_END
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 7, 'TIME_AVAIL_BEG'] = df_atm_wd.SUN_BEG
    df_atm_wd.loc[df_atm_wd.WEEKDAY == 7, 'TIME_AVAIL_END'] = df_atm_wd.SUN_END

    set_group_func(df_atm_wd, df_atm_wd.TIME_AVAIL_BEG, 'TD_AVAIL_BEG', time_to_td)
    set_group_func(df_atm_wd, df_atm_wd.TIME_AVAIL_END, 'TD_AVAIL_END', time_to_td)

    # Получение данных в разрезе дней
    # df_atm_sw = pd.merge(df_atm_wd, df_day, 'outer', on='WEEKDAY').set_index(['ATM_REF', 'DAY'], False).sort_index()
    df_atm_sw = pd.merge(df_atm_wd, df_day, on='WEEKDAY').set_index(['ATM_REF', 'DAY'], False).sort_index()
    # print(df_atm_sw.info())

    # Приведение к datetime
    df_atm_sw['SERVICE_BEG'] = df_atm_sw.DAY + df_atm_sw.TD_SERVICE_BEG
    df_atm_sw['SERVICE_END'] = df_atm_sw.DAY + df_atm_sw.TD_SERVICE_END
    df_atm_sw['AVAIL_BEG'] = df_atm_sw.DAY + df_atm_sw.TD_AVAIL_BEG
    df_atm_sw['AVAIL_END'] = df_atm_sw.DAY + df_atm_sw.TD_AVAIL_END

    # Обединение режимов обслуживания и доступности
    cond = df_atm_sw['AVAIL_BEG'] > df_atm_sw['SERVICE_BEG']
    df_atm_sw.loc[cond, 'SW_BEG'] = df_atm_sw.loc[cond, 'AVAIL_BEG']
    df_atm_sw.loc[~cond, 'SW_BEG'] = df_atm_sw.loc[~cond, 'SERVICE_BEG']

    cond = df_atm_sw['AVAIL_END'] < df_atm_sw['SERVICE_END']
    df_atm_sw.loc[cond, 'SW_END'] = df_atm_sw.loc[cond, 'AVAIL_END']
    df_atm_sw.loc[~cond, 'SW_END'] = df_atm_sw.loc[~cond, 'SERVICE_END']

    cond = df_atm_sw['SERVICE_BEG'] >= df_atm_sw['SERVICE_END']
    df_atm_sw.loc[cond, 'SW_BEG'] = df_atm_sw.loc[cond, 'DAY']
    df_atm_sw.loc[cond, 'SW_END'] = df_atm_sw.loc[cond, 'DAY']

    df_atm_sw['SW_TIME'] = df_atm_sw['SW_END'] - df_atm_sw['SW_BEG']

    # print(df_atm_sw)
    # df_atm_sw.to_csv('data/df_atm_sw.csv', sep='\t')
    return df_atm_sw


def calc_idle(reader, date_beg, date_end):

    df_atm_all = reader.get_atm()
    df_service = reader.get_service(date_beg, date_end)
    df_order = reader.get_orders(date_beg, date_end)

    df_atm = df_service.merge(df_atm_all, on=['ATM_REF'])   #.set_index('ATM_REF', False)

    # Список дат для анализа
    days = dr.date_list(date_beg, date_end)
    df_day = pd.DataFrame({'DAY':[d for d in days]})
    df_day.DAY = pd.to_datetime(df_day.DAY)                 # Приведение к datetime
    df_day['WEEKDAY'] = df_day.DAY.dt.weekday + 1           # Опорное поле дней недели

    df_atm_sw = get_service_window(df_atm, df_day)          # Рсчет сервисного окна для каждого УС

    # Расчет времени ремонта УС по дням
    df_order_day = get_order_day(df_order, df_day)
    df_atm_order = pd.merge(df_atm_sw, df_order_day, on=['ATM_REF', 'DAY']) #.set_index(['ATM_REF', 'DAY'], False)
    df_in_repair = get_in_repair(df_atm_order)              # Длительная операция

    # Расчет времени доступности УС по дням
    df_idle = pd.merge(
        df_atm_sw[['ATM_REF', 'DAY', 'SW_BEG', 'SW_END', 'SW_TIME']],
        df_in_repair[['ATM_REF', 'DAY', 'REPAIR_BEG', 'REPAIR_END']],
        how='left',
        on=['ATM_REF', 'DAY']
    )
    df_idle['REPAIR_TIME'] = df_idle['REPAIR_END'] - df_idle['REPAIR_BEG']
    df_idle.loc[df_idle.REPAIR_TIME.isnull() & (df_idle.SW_TIME > pd.to_timedelta(0)), 'REPAIR_TIME'] = pd.to_timedelta(0)
    df_idle['AVAIL'] = 1 - (df_idle['REPAIR_TIME'] / df_idle['SW_TIME'])
    df_idle['DAY_DATE'] = df_idle.DAY.dt.date
    df_idle['DAY_MONTH'] = df_idle.DAY.dt.strftime('%Y.%m')
    df_idle['DAY_NUM'] = df_idle.DAY.dt.day

    # print(df_idle.info())
    # df_idle.to_csv('data/df_idle.csv', sep='\t')

    df_atm_idle = df_atm[['ATM_REF', 'SERIAL', 'CITY', 'ADDR', 'MODEL']].merge(
        df_idle,
        how='left',
        on=['ATM_REF']
        # left_index=True,
        # right_index=True
    )

    df_atm_idle_pivot = df_atm_idle.pivot_table(index=['ATM_REF', 'SERIAL', 'CITY', 'ADDR', 'MODEL'], columns=['DAY_MONTH', 'DAY_NUM'], values=['AVAIL'])   #.reset_index()   #.set_index(['ATM_REF'])
    print(df_atm_idle_pivot)

    return df_atm_idle_pivot

