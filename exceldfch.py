import pandas as pd
import os 
from clickhouse_driver import Client

client = Client(host='localhost', settings={'use_numpy': True})
client.execute('CREATE DATABASE team_202401')
client.execute('CREATE TABLE team_202401.new_table (num Int64, col1 String, col2 String, col3 String, col4 String, col5 String, col6 String, col7 String, col8 String, col9 String, sheet String) ENGINE MergeTree ORDER BY sheet')

path = os.getcwd()+"/ExcelDFCH/xlsx/"

sheets_dict = pd.read_excel(path+'Book1.xlsx', sheet_name=None)

all_sheets = []
start_id = 0
for name, sheet in sheets_dict.items():
    sheet['sheet'] = name
    sheet = sheet.rename(columns=lambda x: x.split('\n')[-1])
    all_sheets.append(sheet)

full_table = pd.concat(all_sheets)
full_table.reset_index(inplace=True, drop=True)

client.insert_dataframe(
    'INSERT INTO team_202401.new_table VALUES',
    pd.DataFrame(full_table, columns=['num','col1','col2','col3','col4','col5','col6','col7','col8','col9','sheet'])
)


# REF
# https://stackoverflow.com/questions/68830640/how-to-insert-a-df-into-a-clickhouse-table-if-data-type-is-uuid-and-list-of-dat
# https://stackoverflow.com/questions/44549110/python-loop-through-excel-sheets-place-into-one-df

# TO DO
# 1. set datetime record
# 2. OO