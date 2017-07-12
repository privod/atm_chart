import datetime as dt

# import project.scatter

from project.pd.datasource.orcl import DataFrameReaderOrcl
from project.idle import calc_idle

# project.scatter.test()

# df = pd.read_csv('data/kp_all_movies.csv') #скачиваем подготовленные данные


date_beg = dt.date(2017, 5, 1)
date_end = dt.date(2017, 5, 31)

reader = DataFrameReaderOrcl()

calc_idle(reader, date_beg, date_end)
