"""
## ETL DAG loading data from the HH API to a Minio bucket
"""

import asyncio
import json
import os
from datetime import datetime, timedelta
import logging

import requests
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from src.extract.hh_api import HHAsyncClient
from src.storage.raw_layer import S3RawLayerStorage

logger = logging.getLogger(__name__)

# ------------------- #
# DAG-level variables #
# ------------------- #

DAG_ID = os.path.basename(__file__).replace(".py", "")

_AWS_CONN_ID = os.getenv("MINIO_CONN_ID", "minio_local")
_S3_BUCKET = os.getenv("S3_BUCKET", "open-meteo-etl")

_POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
_POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "postgres")
_POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
_SQL_DIR = os.path.join(
    os.path.dirname(__file__), f"../../include/sql/pattern_dags/{DAG_ID}"
)

_EXTRACT_TASK_ID = "extract"
_TRANSFORM_TASK_ID = "transform"

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2024, 9, 23),  # date after which the DAG can be scheduled
    schedule="@daily",  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,  # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_active_runs=1,  # maximum number of active DAG runs
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args={
        "owner": "me",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": timedelta(seconds=30),  # tasks wait 30s in between retries
    },
    tags=["Patterns", "ETL", "Postgres", "Intermediary Storage"],  # add tags in the UI
    params={
        "search_input": Param("python data engineer", type="string"),
    },  # Airflow params can add interactive options on manual runs. See: https://www.astronomer.io/docs/learn/airflow-params
    template_searchpath=[_SQL_DIR],  # path to the SQL templates
)
def etl_intermediary_storage():

    # ---------------- #
    # Task Definitions #
    # ---------------- #
    # the @task decorator turns any Python function into an Airflow task
    # any @task decorated function that is called inside the @dag decorated
    # function is automatically added to the DAG.
    # if one exists for your use case you can use traditional Airflow operators
    # and mix them with @task decorators. Checkout registry.astronomer.io for available operators
    # see: https://www.astronomer.io/docs/learn/airflow-decorators for information about @task
    # see: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators

    @task_group
    def tool_setup():
        _create_bucket_if_not_exists = S3CreateBucketOperator(
            task_id="create_bucket_if_not_exists",
            bucket_name=_S3_BUCKET,
            aws_conn_id=_AWS_CONN_ID,
        )

        return _create_bucket_if_not_exists

    _tool_setup = tool_setup()

    @task(task_id=_EXTRACT_TASK_ID)
    def extract(**context):
        """
        Extract data from the HH API
        Returns:
            dict: The full API response
        """
        async def _async_extract():
            search_input = context["params"]["search_input"]
            dag_run_timestamp = context["ts"]
            dag_id = context["dag"].dag_id
            task_id = context["task"].task_id

            # Initialize storage
            storage = S3RawLayerStorage(
                bucket="raw-layer",
                metadata_db_conn_str="postgresql://postgres:postgres@postgres:5432/jobs_meta",
                endpoint_url="http://minio:9000",  # MinIO endpoint
                aws_access_key_id="minio_access_key",
                aws_secret_access_key="minio_secret_key"
            )
            await storage.initialize()

            # Initialize source
            config = {
                'user_agent': 'curl/7.64.1'
            }

            source = HHAsyncClient(config)
            try:
                # Generate batch ID
                batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

                # Store postings
                async for metadata in storage.store_postings(
                        source.fetch_postings(search_input),
                        batch_id
                ):
                    logger.info(f"Stored posting {metadata.posting_id} in s3://{storage.bucket}/{metadata.s3_key}")

                # Implement retention policy
                await storage.implement_retention_policy(days=7)
            finally:
                await source.close()

        asyncio.run(_async_extract())
        # response = requests.get(url).json()
        #
        # response_bytes = json.dumps(response).encode("utf-8")
        #
        # # Save the data to S3
        # hook = S3Hook(aws_conn_id=_AWS_CONN_ID)
        # hook.load_bytes(
        #     bytes_data=response_bytes,
        #     key=f"{dag_id}/{task_id}/{dag_run_timestamp}.json",
        #     bucket_name=_S3_BUCKET,
        #     replace=True,
        # )

#     @task(task_id=_TRANSFORM_TASK_ID)
#     def transform(**context):
#         """
#         Transform the data
#         Args:
#             api_response (dict): The full API response
#         Returns:
#             dict: The transformed data
#         """
#
#         dag_run_timestamp = context["ts"]
#         dag_id = context["dag"].dag_id
#         upstream_task_id = _EXTRACT_TASK_ID
#         task_id = context["task"].task_id
#
#         # Load the data from S3
#         hook = S3Hook(aws_conn_id=_AWS_CONN_ID)
#         response = hook.read_key(
#             key=f"{dag_id}/{upstream_task_id}/{dag_run_timestamp}.json",
#             bucket_name=_S3_BUCKET,
#         )
#         api_response = json.loads(response)
#
#         time = api_response["hourly"]["time"]
#
#         transformed_data = {
#             "temperature_2m": api_response["hourly"]["temperature_2m"],
#             "relative_humidity_2m": api_response["hourly"]["relative_humidity_2m"],
#             "precipitation_probability": api_response["hourly"][
#                 "precipitation_probability"
#             ],
#             "timestamp": time,
#             "date": [
#                 datetime.strptime(x, "%Y-%m-%dT%H:%M").date().strftime("%Y-%m-%d")
#                 for x in time
#             ],
#             "day": [datetime.strptime(x, "%Y-%m-%dT%H:%M").day for x in time],
#             "month": [datetime.strptime(x, "%Y-%m-%dT%H:%M").month for x in time],
#             "year": [datetime.strptime(x, "%Y-%m-%dT%H:%M").year for x in time],
#             "last_updated": [dag_run_timestamp for i in range(len(time))],
#             "latitude": [api_response["latitude"] for i in range(len(time))],
#             "longitude": [api_response["longitude"] for i in range(len(time))],
#         }
#
#         transformed_data_bytes = json.dumps(transformed_data).encode("utf-8")
#
#         # Save the data to S3
#         hook = S3Hook(aws_conn_id=_AWS_CONN_ID)
#         hook.load_bytes(
#             bytes_data=transformed_data_bytes,
#             key=f"{dag_id}/{task_id}/{dag_run_timestamp}.json",
#             bucket_name=_S3_BUCKET,
#             replace=True,
#         )
#
#     @task
#     def load(**context):
#         """
#         Load the data to the destination without using a temporary CSV file.
#         Args:
#             transformed_data (dict): The transformed data
#         """
#         import csv
#         import io
#
#         from airflow.providers.postgres.hooks.postgres import PostgresHook
#
#         dag_run_timestamp = context["ts"]
#         dag_id = context["dag"].dag_id
#         upstream_task_id = _TRANSFORM_TASK_ID
#
#         # Load the data from S3
#         hook = S3Hook(aws_conn_id=_AWS_CONN_ID)
#         response = hook.read_key(
#             key=f"{dag_id}/{upstream_task_id}/{dag_run_timestamp}.json",
#             bucket_name=_S3_BUCKET,
#         )
#         api_response = json.loads(response)
#
#         # Load the data to Postgres
#         hook = PostgresHook(postgres_conn_id=_POSTGRES_CONN_ID)
#
#         csv_buffer = io.StringIO()
#         writer = csv.writer(csv_buffer)
#         writer.writerow(api_response.keys())
#         rows = zip(*api_response.values())
#         writer.writerows(rows)
#
#         csv_buffer.seek(0)
#
#         with open(f"{_SQL_DIR}/copy_insert.sql") as f:
#             sql = f.read()
#         sql = sql.replace("{schema}", _POSTGRES_SCHEMA)
#         sql = sql.replace("{table}", _POSTGRES_TRANSFORMED_TABLE)
#
#         conn = hook.get_conn()
#         cursor = conn.cursor()
#         cursor.copy_expert(sql=sql, file=csv_buffer)
#         conn.commit()
#         cursor.close()
#         conn.close()
#
    _extract = extract()
#     _transform = transform()
#     _load = load()
#     chain(_extract, _transform, _load)
#     chain(_tool_setup[0], _load)
#     chain(_tool_setup[1], _extract)
#
#
etl_intermediary_storage()