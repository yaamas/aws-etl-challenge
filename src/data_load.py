import psycopg2
from psycopg2 import sql
from psycopg2.extensions import (
    ISOLATION_LEVEL_AUTOCOMMIT,
    ISOLATION_LEVEL_READ_COMMITTED,
)

# from sqlalchemy import create_engine


def connect_db(user, passw):
    return psycopg2.connect(user=user, password=passw)


def check_db_exists(conn, db):

    cur = conn.cursor()
    cur.execute(
        sql.SQL(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db}'")
    )
    exists = cur.fetchone()

    if not exists:
        return False


def check_table_exist(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT EXISTS(SELECT relname FROM pg_class WHERE relname='covid19_us');"
    )
    if cur.fetchone()[0]:
        return True

    return False


def create_db(conn, db):
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(sql.SQL(f"CREATE DATABASE {db};"))

    conn.commit()
    cur.close()

    conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)

    return None


def create_table(conn):
    cur = conn.cursor()
    cur.execute(
        sql.SQL(
            "CREATE TABLE COVID19_US (id serial, date DATE PRIMARY KEY NOT NULL, cases INTEGER NOT NULL, deaths INTEGER NOT NULL, recovered INTEGER NOT NULL);"
        )
    )
    conn.commit()
    cur.close()


def insert_data(data, data_exists):
    pass


def load(data):

    DB = "test"  # env.
    USER = "test"  # env.
    PASSWORD = "test"  # env.

    # data_exists = False
    conn = connect_db(USER, PASSWORD)

    if not check_db_exists(conn, DB):
        create_db(conn, DB)
        create_table(conn)
        # data_exists = True
    elif not check_table_exist(conn):
        create_table(conn)
        # data_exists = True

    # insert_data(data, data_exists)

    conn.close()


if __name__ == "__main__":
    import pandas as pd

    from data_transform import transformation

    n = pd.read_csv("sample_data/nyt_data.csv")
    j = pd.read_csv("sample_data/jh_data.csv")

    m = transformation(n, j)

    print(m.info(), m.head())
    print(m.iloc[0, :])

    load(m)
