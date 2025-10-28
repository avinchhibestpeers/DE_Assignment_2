from pathlib import Path

def create_schema(sql_script_path: str|Path, conn):
    """
    Execute schema sql script on database.
    """
    if isinstance(sql_script_path, str):
        sql_script_path = Path(sql_script_path)
        
    # conn = connect()

    with conn:
        with conn.cursor() as cur, sql_script_path.open("r") as file:
            cur.execute(file.read())


# create_schema("ddl_script.sql")