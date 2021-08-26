import logging

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import (
    ISOLATION_LEVEL_AUTOCOMMIT,
    ISOLATION_LEVEL_READ_COMMITTED,
)

logger = logging.getLogger(__name__)


def connect_db(host, user, passw, db=None):
    """"""
    if db:
        return psycopg2.connect(
            host=host, dbname=db, user=user, password=passw
        )
    return psycopg2.connect(host=host, user=user, password=passw)


def check_db_exists(conn, db):
    """"""
    try:
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db}'"
            )
        )
    except Exception as e:
        logger.error(e)
        conn.rollback()
    finally:
        exists = cur.fetchone() is not None
        cur.close()

        logger.info(f"The database exists [{exists}]")

    return exists


def check_table_exists(conn, table):
    """"""
    try:
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                f"SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table.lower()}');"
            )
        )
    except Exception as e:
        conn.rollback()
        logger.error(e)
    finally:
        exists = cur.fetchone()[0]
        cur.close()

        logger.info(f"The table exists [{exists}] in the database")

    return exists


def create_db(conn, db):
    """"""
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        cur = conn.cursor()
        cur.execute(sql.SQL(f"CREATE DATABASE {db};"))
    except Exception as e:
        logger.error(e)
    finally:
        conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        cur.close()

        logger.info("Created database successfully")

    return None


def create_table(conn, table):
    """"""
    try:
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                f"CREATE TABLE {table} (id SERIAL PRIMARY KEY, dates DATE UNIQUE NOT NULL, cases INTEGER NOT NULL, deaths INTEGER NOT NULL, recovered INTEGER NOT NULL);"
            )
        )
    except Exception as e:
        logger.error(e)
    finally:
        conn.commit()
        cur.close()

        logger.info("Created table successfully")


def insert_data(conn, data, table):
    """"""

    # Convert to lists for processing next
    data_rows = data.to_records(index=False)

    # Check entries in the table to know what to insert
    try:
        cur = conn.cursor()
        cur.execute(sql.SQL(f"SELECT COUNT(*) FROM {table};"))
    except Exception as e:
        conn.rollback()
        logger.error(e)

    table_rows = cur.fetchone()[0]

    # More than one row missing, don't know which, try inserting all one by one
    if table_rows > 0 and table_rows < (len(data) - 1):
        logger.info(f"More than one row missing - {table_rows}")
        for row in data_rows:
            try:
                cur.execute(
                    sql.SQL(
                        f"INSERT INTO {table} (\"dates\", \"cases\", \"deaths\", \"recovered\") VALUES ('{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}')"
                    )
                )
            except psycopg2.IntegrityError as e:
                conn.rollback()
                logger.debug(e)
                # print("1 - !@#$%^&*()_+-=:", e)
            except Exception as e:
                conn.rollback()
                logger.error(e)
                # print("2 - !@#$%^&*()_+-=:", e)
            finally:
                conn.commit()
                logger.info(
                    f"Insert for: Inserted {len(data_rows)} successfully"
                )

    # One row to insert all all rows to insert
    else:
        logger.info(f"Rows missiing [{table_rows}]")
        if table_rows == (len(data) - 1):
            data_rows = data_rows[-1:]
            logger.info(f"Only 1 row to update, {data_rows}")

        # If all rows missing, insert all rows. Otherwise use updated data_rows
        arg_str = ",".join(
            f"('{da}', '{c}', '{de}', '{r}')" for (da, c, de, r) in data_rows
        )
        try:
            cur.execute(
                sql.SQL(
                    f'INSERT INTO {table} ("dates", "cases", "deaths", "recovered") VALUES '
                    + arg_str
                )
            )
        except Exception as e:
            conn.rollback()
            logger.error(e)
        finally:
            conn.commit()
            logger.info(
                f"Insert Chain: Inserted {len(data_rows)} successfully"
            )

    cur.close()


def load(data):
    """"""
    HOST = "localhost"  # env.
    DB = "covid19"  # env.
    TABLE_NAME = "US"  # env
    USER = "test"  # env.
    PASSWORD = "test"  # env.

    conn = connect_db(HOST, USER, PASSWORD)
    if not (temp := check_db_exists(conn, DB)):
        logger.info(
            f"Database does not exist [{temp}], creating a new database"
        )
        create_db(conn, DB)

    conn.close()

    conn = connect_db(HOST, USER, PASSWORD, DB)
    if not (temp := check_table_exists(conn, TABLE_NAME)):
        logger.info(f"Table does not exist [{temp}], creating a new table")
        create_table(conn, TABLE_NAME)

    insert_data(conn, data, TABLE_NAME)

    conn.close()


if __name__ == "__main__":
    # import pandas as pd

    from data_fetch import fetch_data
    from data_transform import transform_data

    # import time

    n, j = fetch_data()
    print("############### n :", n.info())
    print("############### j :", j.info())

    m = transform_data(n, j)
    print(len(m))

    # print(m.info(), m.head())
    # print(m.iloc[0, :])

    load(m)
