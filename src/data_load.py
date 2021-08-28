import logging

import pandas as pd
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
        try:
            conn = psycopg2.connect(
                host=host, dbname=db, user=user, password=passw
            )
        except Exception:
            logger.exception("Database connection failed - with db param")
    else:
        try:
            conn = psycopg2.connect(host=host, user=user, password=passw)
        except Exception:
            logger.exception("Database connection failed - without db param")

    return conn


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
    except Exception:
        conn.rollback()
        logger.exception("Check table exists failed")
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
    except Exception:
        logger.exception("Create DB failed")
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
    except Exception:
        logger.exception("Create table failed")
    finally:
        conn.commit()
        cur.close()

        logger.info("Created table successfully")


def insert_data(conn, data, table):
    """"""

    # Convert to lists for processing next
    data_rows = data.to_records(True)

    # Check entries in the table to know what to insert
    try:
        cur = conn.cursor()
        cur.execute(sql.SQL(f"SELECT COUNT(*) FROM {table};"))
    except Exception:
        conn.rollback()
        logger.exception("Select count failed")

    table_rows = cur.fetchone()[0]

    logger.info(f"Rows in data: [{len(data_rows)}]")
    logger.info(f"Rows in table: [{table_rows}]")

    # More than one row missing, don't know which, try inserting all one by one
    if (
        table_rows != (len(data) - 1)
        and table_rows > 0
        and table_rows < (len(data) - 1)
    ):
        logger.info(f"More than one row missing - {table_rows}")
        for row in data_rows:
            excep = False
            try:
                cur.execute(
                    sql.SQL(
                        f"INSERT INTO {table} (\"dates\", \"cases\", \"deaths\", \"recovered\") VALUES ('{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}')"
                    )
                )
            except psycopg2.IntegrityError:
                excep = True
                conn.rollback()
                logger.debug(
                    f"Unique constraint violation: {pd.to_datetime(row[1]).date()}"
                )
                # print("1 - !@#$%^&*()_+-=:", e)
            except Exception:
                excep = True
                conn.rollback()
                logger.exception("Insert 'for' failed")
                # print("2 - !@#$%^&*()_+-=:", e)
            finally:
                conn.commit()
                if not excep:
                    logger.info(f"Insert for: Inserted {row[0]} successfully")
    # If rows in table = rows in neww data, do nothing
    elif table_rows == len(data):
        pass
    # One row to insert all all rows to insert
    else:
        excep = False
        if table_rows == (len(data) - 1):
            data_rows = data_rows[-1:]
            logger.info(f"Only 1 row to update, [{data_rows[0][0]}]")

        # If all rows missing, insert all rows. Otherwise use updated data_rows
        arg_str = ",".join(
            f"('{da}', '{c}', '{de}', '{r}')"
            for (_, da, c, de, r) in data_rows
        )
        try:
            cur.execute(
                sql.SQL(
                    f'INSERT INTO {table} ("dates", "cases", "deaths", "recovered") VALUES '
                    + arg_str
                )
            )
        except Exception:
            excep = True
            conn.rollback()
            logger.exception("Insert Chain failed")
        finally:
            conn.commit()
            if not excep:
                logger.info(
                    f"Insert Chain: Inserted [{len(data_rows)}] rows successfully"
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
