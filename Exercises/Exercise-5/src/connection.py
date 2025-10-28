from functools import cache
import psycopg2


@cache
def connect():
    conn = psycopg2.connect(
        host="postgres", port="5432", database="postgres", user="postgres", password="postgres"
    )
    return conn
