import psycopg2
from psycopg2 import sql
from psycopg2.extensions import (
    ISOLATION_LEVEL_AUTOCOMMIT,
    ISOLATION_LEVEL_READ_COMMITTED,
)

# from sqlalchemy import create_engine


def connect_db(host, user, passw, db=None):
    if db:
        return psycopg2.connect(
            host=host, dbname=db, user=user, password=passw
        )
    return psycopg2.connect(host=host, user=user, password=passw)


def check_db_exists(conn, db):

    cur = conn.cursor()
    cur.execute(
        sql.SQL(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db}'")
    )

    exists = cur.fetchone()
    cur.close()

    return exists is not None


def check_table_exists(conn):
    cur = conn.cursor()
    # Take 1
    # cur.execute(
    #     "SELECT EXISTS(SELECT relname FROM pg_class WHERE relname='US');"
    # )
    #
    # Take 2
    # cur.execute("SELECT to_regclass('public.uks');")
    #
    # Take 3
    cur.execute(
        "SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'us');"
    )

    exists = cur.fetchone()[0]
    cur.close()

    # print()

    return exists


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
            "CREATE TABLE US (id SERIAL PRIMARY KEY, date DATE UNIQUE NOT NULL, cases INTEGER NOT NULL, deaths INTEGER NOT NULL, recovered INTEGER NOT NULL);"
        )
    )
    conn.commit()
    cur.close()


def insert_data(conn, data):
    cur = conn.cursor()
    cur.execute(sql.SQL("SELECT COUNT(*) FROM US;"))
    c = cur.fetchone()[0]
    print("insert_data: ", c)

    cur.close()


def load(data):

    HOST = "localhost"  # env.
    DB = "covid19"  # env.
    USER = "test"  # env.
    PASSWORD = "test"  # env.

    conn = connect_db(HOST, USER, PASSWORD)
    if not check_db_exists(conn, DB):
        print("######## db exists: ", check_db_exists(conn, DB))
        create_db(conn, DB)

    conn.close()

    conn = connect_db(HOST, USER, PASSWORD, DB)
    if not check_table_exists(conn):
        print("######## table exists: ", check_table_exists(conn))
        create_table(conn)

    insert_data(conn, data)


if __name__ == "__main__":
    import pandas as pd

    from data_transform import transformation

    n = pd.read_csv("sample_data/nyt_data.csv")
    j = pd.read_csv("sample_data/jh_data.csv")

    m = transformation(n, j)
    print(len(m))

    print(m.info(), m.head())
    # print(m.iloc[0, :])

    load(m)
