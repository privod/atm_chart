import datetime as dt

import pandas as pd
# import project.scatter

from project.pd.datasource.orcl import DataFrameReaderOrcl
from project.idle import calc_idle
from project.view import to_excel

# project.scatter.test()

# df = pd.read_csv('data/kp_all_movies.csv') #скачиваем подготовленные данные


date_beg = dt.date(2017, 5, 1)
date_end = dt.date(2017, 5, 31)

# reader = DataFrameReaderOrcl()
# df_idle = calc_idle(reader, date_beg, date_end)
# # Сохранение расчета
# df_idle.to_pickle('data/df_idle.pd', compression='xz')

# Загрузка расчета
df_idle = pd.read_pickle('data/df_idle.pd', compression='xz')
# df_in_repair = df_in_repair.set_index(['ATM_REF', 'DAY'], False)
# print(df_in_repair.head())

to_excel(df_idle)