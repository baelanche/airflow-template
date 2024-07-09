from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
   'owner': 'baelanche',
   'start_date': datetime(2024, 7, 2, hour=0, minute=00),
   'email': ['baelanche@gmail.com'],
   'retries': 0,
   'retry_delay': timedelta(minutes=3),
}

@task
def get_snowflake_data():
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake-testaccount")

    query = """
        SELECT * FROM table LIMIT 1000000
    """

    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

    return result

@task
def full_refresh_mssql(data):
    mssql_hook = MsSqlHook(mssql_conn_id="mssql-dev-test")

    try:
        with mssql_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM dbo.airflow_target")
                cursor.executemany("""
                    INSERT INTO dbo.airflow_target (id, title, desc)
                    VALUES (%s, %s, %s)
                """, data)
                conn.commit()
                logger.info(f'inserted {len(data)} rows')
    except Exception as e:
        conn.rollback()
        logger.error(f'Failed to insert into MSSQL: {str(e)}')
        raise

with DAG(
    "sample_snowflake_naive",
    schedule=None,
    tags=['test'],
    catchup=False,
    default_args=default_args
) as dag:
    full_refresh_mssql(get_snowflake_data())