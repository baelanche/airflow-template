from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime, timedelta
import logging
import csv
import tempfile
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
   'owner': 'baelanche',
   'start_date': datetime(2024, 7, 2, hour=0, minute=00),
   'email': ['baelanche@gmail.com'],
   'retries': 0,
   'retry_delay': timedelta(minutes=3),
}

container_name = 'container_name'
blob_name = 'blob_name'

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
def upload_to_blob(data):
    blob_path = 'csv'

    temp_csv_path = tempfile.NamedTemporaryFile(suffix='.csv', delete=False).name
    with open(temp_csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(data)

    blob_hook = WasbHook(wasb_conn_id='blob-testaccount')

    try:
        blob_hook.delete_file(
            container_name=container_name,
            blob_name=blob_name,
            ignore_if_missing=True
        )
    except Exception as e:
        logger.error(f'Failed to delete blob {blob_name} from container {container_name}: {str(e)}')
        raise

    try:
        with open(temp_csv_path, 'rb') as file:
            blob_hook.upload(
                container_name=container_name,
                blob_name=blob_name,
                data=file,
                blob_type='BlockBlob',
                create_container=True
            )
    except Exception as e:
        logger.error(f'Failed to upload to Azure Blob Storage: {str(e)}')
        raise
    finally:
        if temp_csv_path and os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)

    return f'{blob_path}/{blob_name}'

@task
def full_refresh_mssql(blob_path):
    mssql_hook = MsSqlHook(mssql_conn_id="mssql-dev-test")

    try:
        with mssql_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM dbo.airflow_target")
                cursor.execute(f"""
                    BULK INSERT dbo.airflow_target
                    FROM '{blob_name}
                    WITH (
                        DATA_SOURCE = 'testDataSource',
                        FORMAT = 'CSV',
                        CODEPAGE = '65001',
                        FIRSTROW = 1,
                        ROWTERMINATOR = '\n',
                        FIELDTERMINATOR = ','
                    )
                """)
                conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f'Failed to insert into MSSQL: {str(e)}')
        raise

with DAG(
    "sample_snowflake_blob",
    schedule=None,
    tags=['test'],
    catchup=False,
    default_args=default_args
) as dag:
    full_refresh_mssql(upload_to_blob(get_snowflake_data()))