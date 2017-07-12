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


def calc_idle(reader, date_beg, date_end):

    df_atm_all = reader.get_atm()
    df_service = reader.get_service(date_beg, date_end)
    df_order = reader.get_orders(date_beg, date_end)

    df_atm = df_service.merge(df_atm_all, on=['ATM_REF']).set_index('ATM_REF', False)

    # Список дат для анализа
    days = dr.date_list(date_beg, date_end)
    df_day = pd.DataFrame({'DAY':[d for d in days]})

    df_order_day = get_order_day(df_order, df_day)

# --- Рсчет сервисного окна для каждого УС ------------------------------------
    df_atm_sw = df_cartesian(df_atm, df_day).set_index(['ATM_REF', 'DAY'], False)

    df_atm_sw['SW_BEG'] = df_atm_sw.A_TIME_BEG
    df_atm_sw['SW_END'] = df_atm_sw.A_TIME_END

    df_atm_sw.loc[pd.to_datetime(df_atm_sw.DAY).dt.weekday + 1 > df_atm_sw.A_DAYS, ['SW_BEG', 'SW_END']] = [dt.time(0), dt.time(0)]

    # TODO Необходимо учитывать режим доступности УС

# -----------------------------------------------------------------------------
    df_atm_order = pd.merge(df_atm_sw, df_order_day, on=['ATM_REF', 'DAY']).set_index('ATM_REF', 'DAY', False)
    df_atm_order.to_csv('data/df_atm_order.csv', sep='\t')

    i = 0
    in_repair_list = []
    grp_atm_order = df_atm_order.groupby(['ATM_REF', 'DAY'])[['SW_BEG', 'SW_END', 'DATE_REG', 'DATE_END']]
    bar = ProgressBar(max_value=len(grp_atm_order.size())).start()
    for (atm_ref, day), grp in grp_atm_order:

        i += 1
        in_repair_day = []
        for order in grp.itertuples():
            service_win = (pd.datetime.combine(day, order.SW_BEG), pd.datetime.combine(day, order.SW_END))

            if service_win is not None:
                (beg, end) = dr.inner_join([service_win, (order.DATE_REG, order.DATE_END)])
                if None not in [beg, end]:
                    in_repair_day.append((beg, end))

            if i % 100 == 0:
                bar.update(i)

        in_repair_list.extend([(atm_ref, day, *item) for item in dr.outer_join(in_repair_day) if item is not None])

    bar.finish()

    df_in_repair = pd.DataFrame(in_repair_list, columns=['ATM_REF', 'DAY', 'REPAIR_BEG', 'REPAIR_END']).set_index('ATM_REF', 'DAY', False)
    df_in_repair.to_csv('data/df_in_repair.csv', sep='\t')
