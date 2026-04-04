from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

from src.orchestration.airflow.python_script.postgre_to_bq import main

JOB_CONFIG_PATH = "/opt/airflow/dags/etl_pipeline_dags/src/config/postgre_to_bq/market_data/foreign_trade.yaml"

with DAG(
    dag_id='market_data_foreign_trade_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='run_silver_market_data_foreign_trade_jobs',
        python_callable=main,
        op_kwargs={
            "job_config_path": JOB_CONFIG_PATH
        },
    )
    
