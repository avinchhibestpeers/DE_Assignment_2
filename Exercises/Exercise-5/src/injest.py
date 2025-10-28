def injest_table(
        table_name: str, 
        fieldnames: list[str], 
        data: list[tuple],
        conn
    ):
    query_field_str = f"({",".join(fieldnames)})"
    values_str = f"({','.join(['%s']*len(fieldnames))})"

    query = f"insert into {table_name} {query_field_str} values {values_str};"

    with conn:
        with conn.cursor() as curr:
            curr.executemany(query, data)
            # conn.commit()


# from src.extract_and_clean import CSVExtractor

# extractor = CSVExtractor("data/accounts.csv")
# fieldnames = extractor.fieldnames
# data = extractor.csv_to_tuples()

# injest_table("accounts", fieldnames, data, connect())