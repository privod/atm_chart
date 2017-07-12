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

    # df_res = df_order.merge(, on=)

    # df_res = pd.date_range(df_order.DATE_REG, df_order.DATE_END)
    # print(df_res)


def calc_idle(reader, date_beg, date_end):

    df_atm_all = reader.get_atm()
    df_service = reader.get_service(date_beg, date_end)
    df_order = reader.get_orders(date_beg, date_end)

    df_atm = df_service.merge(df_atm_all, on=['ATM_REF']).set_index('ATM_REF', False)
    # print(df_atm)

    # Список дат для анализа
    days = dr.date_list(date_beg, date_end)
    df_day = pd.DataFrame({'DAY':[d for d in days]})
    # print(df_day)

    df_order_day = get_order_day(df_order, df_day)

# --- Рсчет сервисного окна для каждого УС ------------------------------------
    df_atm_sw = df_cartesian(df_atm, df_day).set_index(['ATM_REF', 'DAY'], False)
    # print(df_atm_sw)

    # df_atm_sw['SW_BEG'] = pd.to_datetime(df_atm_sw.DAY) + pd.to_timedelta(df_atm_sw.A_TIME_BEG.astype(str))
    # df_atm_sw['SW_END'] = pd.to_datetime(df_atm_sw.DAY) + pd.to_timedelta(df_atm_sw.A_TIME_END.astype(str))
    # # print(df_atm_sw)
    # df_atm_sw.loc[df_atm_sw.SW_BEG.dt.weekday + 1 > df_atm_sw.A_DAYS, ['SW_BEG']] = pd.to_datetime(df_atm_sw.DAY)
    # df_atm_sw.loc[df_atm_sw.SW_END.dt.weekday + 1 > df_atm_sw.A_DAYS, ['SW_END']] = pd.to_datetime(df_atm_sw.DAY)

    df_atm_sw['SW_BEG'] = df_atm_sw.A_TIME_BEG
    df_atm_sw['SW_END'] = df_atm_sw.A_TIME_END

    # print(pd.to_datetime(df_atm_sw.DAY).dt.weekday)

    df_atm_sw.loc[pd.to_datetime(df_atm_sw.DAY).dt.weekday + 1 > df_atm_sw.A_DAYS, ['SW_BEG', 'SW_END']] = [dt.time(0), dt.time(0)]

    # TODO Необходимо учитывать режим доступности УС

    # df_atm_sw.to_csv('data/df_res.csv', sep='\t')
# -----------------------------------------------------------------------------

    # s = Series([1, 2, 3, 4, 5])
    # print(s)
    # bs = (s > 2) & (s < 5)
    # print(bs)
    #
    # df1 = df_order.ATM_REF == 2222
    # df2 = df_order.ATM_REF == 33333
    # print(df1)

    df_atm_order = pd.merge(df_atm_sw, df_order_day, on=['ATM_REF', 'DAY']).set_index('ATM_REF', 'DAY', False)
    df_atm_order.to_csv('data/df_atm_order.csv', sep='\t')
    # print(df_res)


    # def test(grp):
    #     in_repair_day = []
    #     for order in grp.itertuples():
    #         # print(order)
    #         service_win = (pd.datetime.combine(order.DAY, order.SW_BEG), pd.datetime.combine(order.DAY, order.SW_END))
    #
    #         if service_win is not None:
    #             in_repair = dr.inner_join([service_win, (order.DATE_REG, order.DATE_END)])
    #             if in_repair is not None:
    #                 in_repair_day.append(in_repair)
    #
    #     in_repair_list = dr.outer_join(in_repair_day)
    #     return Series(in_repair_list)
    #
    # df_res = df_atm_order.groupby(['ATM_REF', 'DAY']).apply(test)
    # # print(df_res)

    i = 0
    in_repair_list = []
    grp_atm_order = df_atm_order.groupby(['ATM_REF', 'DAY'])[['SW_BEG', 'SW_END', 'DATE_REG', 'DATE_END']]
    bar = ProgressBar(max_value=len(grp_atm_order.size())).start()
    for (atm_ref, day), grp in grp_atm_order:
        i += 1
        in_repair_day = []
        for order in grp.itertuples():
            # print(order)
            service_win = (pd.datetime.combine(day, order.SW_BEG), pd.datetime.combine(day, order.SW_END))

            if service_win is not None:
                (beg, end) = dr.inner_join([service_win, (order.DATE_REG, order.DATE_END)])
                if None not in [beg, end]:
                    in_repair_day.append((beg, end))

            if i % 100 == 0:
                bar.update(i)

        # print(in_repair_day)
        in_repair_list.extend([(atm_ref, day, *item) for item in dr.outer_join(in_repair_day) if item is not None])
    bar.finish()

    df_in_repair = pd.DataFrame(in_repair_list, columns=['ATM_REF', 'DAY', 'REPAIR_BEG', 'REPAIR_END']).set_index('ATM_REF', 'DAY', False)
    # df_in_repair = pd.DataFrame(in_repair_list)
    # print(df_in_repair)
    df_in_repair.to_csv('data/df_in_repair.csv', sep='\t')

    # for row in df_atm_sw.itertuples():
    #     ord = df_order[(df_order.ATM_REF == row.ATM_REF) & (df_order.DATE_REG <= row.DAY) & (row.DAY <= df_order.DATE_END)]
        # if not ord.empty:
        #     print(row)
        #     print(ord)

    # def test(v):
    #     return len(v)
    #
    # df_atm_sw = df_order.merge(df_atm, on='ATM_REF', suffixes=('_ORDER', '_ATM')).apply(test)


    # def aggrfunc_test(v):
    #     return len(v)
    #
    # grp = df_res.groupby(by=['ATM_REF', 'SERIAL', 'ATM_TYPE', 'A_NUMBER_ATM', 'PRODUCER', 'MODEL', 'CITY', 'ADDR']).apply(aggrfunc_test)
    # pvt = df_res.pivot_table(index=['ATM_REF', 'SERIAL', 'ATM_TYPE', 'A_NUMBER_ATM', 'PRODUCER', 'MODEL', 'CITY', 'ADDR'], values=['REF'], aggfunc=aggrfunc_test)


    # day = dt.date(2017, 4, 1)
    # print(day)


    # df_work_time = df_atm.apply(lambda atm, day: pd.Series(get_work_time(atm, day), index=['WBEG', 'WEND']), axis=1, args=[day])
    # print(df_work_time)




