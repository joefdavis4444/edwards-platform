from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'edwards-platform',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='edwards_batch_pipeline',
    description='Daily batch pipeline: generate → GCS raw → curated → BigQuery',
    default_args=default_args,
    start_date=datetime(2026, 4, 27),
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['edwards', 'batch'],
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command='python /home/airflow/gcs/dags/ingestion/batch/generate_data.py',
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/tmp/raw/*.csv',
        dst='raw/',
        bucket='edwards-datalake',
        gcp_conn_id='google_cloud_default',
    )

    run_batch_pipeline = BashOperator(
        task_id='run_batch_pipeline',
        bash_command='python /home/airflow/gcs/dags/pipelines/batch/batch_pipeline.py',
    )

    run_bq_load = BashOperator(
        task_id='run_bq_load',
        bash_command='python /home/airflow/gcs/dags/pipelines/batch/bq_load_pipeline.py',
    )

    generate_data >> upload_to_gcs >> run_batch_pipeline >> run_bq_load
