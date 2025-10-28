from pathlib import Path

from src.connection import connect
from src.schema import create_schema
from src.extract_and_clean import CSVExtractor
from src.injest import injest_table



def main():
    conn = connect()
    sql_script_file = 'ddl_script.sql'

    create_schema(sql_script_file, conn)
    csv_files = [
        Path('data/accounts.csv'),
        Path("data/products.csv"),
        Path("data/transactions.csv")
    ]
    
    for csv_file in csv_files:
        extractor = CSVExtractor(csv_file)

        filenames = extractor.fieldnames
        data = extractor.csv_to_tuples()
        table_name = csv_file.stem

        injest_table(table_name, filenames, data, conn)

    conn.close()

    print("Data injested successfully.")

if __name__ == "__main__":
    main()
