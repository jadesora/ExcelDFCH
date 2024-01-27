import pandas as pd
import os
from clickhouse_driver import Client

class ExcelToClickHouseLoader:
    def __init__(self, table_cols, insert_cols, db_name, excel_file_path):
        self.table_cols = table_cols
        self.insert_cols = insert_cols
        self.db_name = db_name
        self.excel_file_path = excel_file_path
        self.path = os.getcwd() + "/ExcelDFCH/xlsx/"
        self.sheets_dict = pd.read_excel(self.path + self.excel_file_path, sheet_name=None)
        self.client = Client(host='localhost', settings={'use_numpy': True})

    def create_database(self):
        self.client.execute('DROP DATABASE IF EXISTS ' + self.db_name)
        self.client.execute('CREATE DATABASE ' + self.db_name)

    def create_table(self):
        columns_definition = ', '.join(f'{col} Int64' if col == 'num' else f'{col} String' for col in self.table_cols)
        table_creation_query = (
            f'CREATE TABLE {self.db_name}.new_table ({columns_definition}) '
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
        self.client.insert_dataframe(insert_query, pd.DataFrame(full_table, columns=self.insert_cols))
