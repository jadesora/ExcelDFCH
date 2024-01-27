import pandas as pd
import os
from clickhouse_driver import Client

class ExcelToClickHouseLoader:
    def __init__(self, cols, db_name, excel_file_path):
        self.cols = cols
        self.db_name = db_name
        self.excel_file_path = excel_file_path
        self.path = os.getcwd() + "/ExcelDFCH/xlsx/"
        self.sheets_dict = pd.read_excel(self.path + self.excel_file_path, sheet_name=None)
        self.client = Client(host='localhost', settings={'use_numpy': True})

    def create_database(self):
        self.client.execute('DROP DATABASE IF EXISTS ' + self.db_name)
        self.client.execute('CREATE DATABASE ' + self.db_name)

    def create_table(self):
        table_creation_query = (
            f'CREATE TABLE {self.db_name}.new_table '
            '(num Int64, col1 String, col2 String, col3 String, col4 String, '
            'col5 String, col6 String, col7 String, col8 String, col9 String, sheet String) '
            'ENGINE MergeTree ORDER BY sheet'
        )
        self.client.execute(table_creation_query)

    def process_sheets(self):
        all_sheets = []
        start_id = 0
        for name, sheet in self.sheets_dict.items():
            sheet['sheet'] = name
            sheet = sheet.rename(columns=lambda x: x.split('\n')[-1])
            all_sheets.append(sheet)

        full_table = pd.concat(all_sheets)
        full_table.reset_index(inplace=True, drop=True)

        insert_query = f'INSERT INTO {self.db_name}.new_table VALUES'
        self.client.insert_dataframe(insert_query, pd.DataFrame(full_table, columns=self.cols))
