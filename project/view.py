import pandas as pd


def to_excel(df_idle):

    sheet_name = 'Динамика доступности'
    writer = pd.ExcelWriter('data/df_idle.xls', engine='xlsxwriter', date_format='dd.mm.yyyy')

    df_idle.to_excel(writer, sheet_name=sheet_name, startcol=0, startrow=1, index=False)

    # format_day = writer.book.add_format({'num_format': 'dd'})

    # sheet = writer.sheets[sheet_name]
    # sheet.add_table(1, 0, 10, 3000)
    # sheet.conditional_format(1, 5, 31,  cell_format=format_day)

    writer.save()

    # df_idle_view.to_csv('data/df_idle_pivot.csv', sep='\t')
    # print(df_idle_view)
