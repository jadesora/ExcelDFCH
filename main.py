from ExcelToClickHouseLoader.loader import ExcelToClickHouseLoader

if __name__ == "__main__":
    db_name = 'team_202402'
    table_cols = ['num', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'sheet']
    insert_cols = ['num', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'sheet']
    excel_file_path = 'Book1.xlsx'

    excel_loader = ExcelToClickHouseLoader(table_cols, insert_cols, db_name, excel_file_path)
    excel_loader.create_database()
    excel_loader.create_table()
    excel_loader.process_sheets()
