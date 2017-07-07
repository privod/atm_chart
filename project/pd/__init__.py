import pandas as pd


def df_cartesian(df1, df2):
    df1['key'] = 0
    df2['key'] = 0
    res = pd.merge(df1, df2, 'outer', on='key')
    res.drop('key', axis=1, inplace=True)

    return res