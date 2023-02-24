from airflow import DAG
from airflow.operators.dummy import DummyOperator
from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
)

from datetime import datetime, timedelta


default_args = {
    "owner": "Airflow",
    "start_date": datetime(2021, 4, 6),
    "dbt_cloud_conn_id": "dbt_cloud", 
    "account_id": 75082
}

with DAG(
    dag_id="ad_reporting_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

 #   google_sheet_sync = FivetranOperator(
 #       task_id="google_sheet-sync",
 #       connector_id="{{ var.value.google_sheet_connector_id }}",
 #   )

    google_sheet_sensor = FivetranSensor(
        task_id="google_sheet-sensor",
        connector_id="{{ var.value.google_sheet_connector_id }}",
        poke_interval=30,
    )
        
    trigger_dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_run",
        job_id=207695,
        check_interval=10,
        timeout=300,
    )


    google_sheet_sensor >> trigger_dbt_cloud_job_run