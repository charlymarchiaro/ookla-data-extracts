from datetime import datetime, timedelta
import psycopg2  # type: ignore
import psycopg2.extras  # type: ignore
import os

import logging

import scripts.consts as consts


def get_db_connection():
    server = os.environ.get("DB_SERVER")
    port = os.environ.get("DB_PORT")
    db_name = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")

    return psycopg2.connect(
        user=user, password=password, host=server, port=port, database=db_name,
    )


def get_android_bg_v2_stored_dates() -> list[datetime]:
    conn = get_db_connection()
    cursor = conn.cursor()

    # Delete if exists the data for the file date
    sql = f"""
                SELECT DISTINCT
                        received_date::date 
                FROM
                        public.android_bg_v2
                ORDER BY
                        1
            """
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows]


def remove_outdated_files(total_stored_days: int):
    logging.info("Removing outdated files from the database...")

    file_date_str = (datetime.today() - timedelta(total_stored_days + 1)).strftime(
        "%Y-%m-%d"
    )

    conn = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Delete if exists the data for the file date
        sql = f"""
                    DELETE FROM
                            public.android_bg_v2
                    WHERE
                            received_date::date < '{file_date_str}'                            
                    """
        cursor.execute(sql)
        conn.close()
        logging.info("--> Success")

    except psycopg2.DatabaseError as error:
        logging.error(error)

    finally:
        if conn is not None:
            conn.close()


def save_android_bg_v2_file(file_path: str):
    logging.info("Saving data in the database. Source file: " + file_path)
    file_date_str = file_path[-14:-4]

    conn = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Delete if exists the data for the file date
        sql = f"""
                DELETE FROM
                        public.android_bg_v2
                WHERE
                        received_date::date = '{file_date_str}'
                """
        cursor.execute(sql)

        # Import the file data
        column_names_str = ",".join(consts.android_bg_v2_column_names)

        norm_file_path = os.path.abspath(file_path)

        sql = f"""
                COPY public.android_bg_v2(
                    {column_names_str}
                )
                FROM '{norm_file_path}'
                WITH (
                    FORMAT CSV,
                    HEADER,
                    DELIMITER ',',
                    QUOTE '\"',
                    ESCAPE '''',
                    FORCE_NULL(
                        {column_names_str}
                    )
                )
                """
        cursor.copy_expert(sql, open(norm_file_path, "r"))
        conn.commit()
        conn.close()
        logging.info("--> Success")

    except psycopg2.DatabaseError as error:
        logging.error(error)

    finally:
        if conn is not None:
            conn.close()
