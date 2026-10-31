import logging
from typing import Any

from airflow.sdk import TaskGroup
from airflow.sdk import Asset
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from etl_pipeline.models.orchestration.task_factories.task_factory import TaskFactoryBase
from etl_pipeline.orchestration.airflow.python_script.postgre_to_databricks_volume_bronze import main
from etl_pipeline.utils.config_loader import load_and_parse_config


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BronzePostgreDatabricksFactory(TaskFactoryBase):
    """
        Load postgresql data to databricks volume
    """
    def _create_task_impl(
        self,
        task_group_id: str,
        dag: Any, args: dict[str, Any]
    ):
        task_id = args["task_id"]
        logger.info("Create bronze postgresql to databricks volume data task groups")

        job_config_path = args["job_config_path"]
        job_config = load_and_parse_config(job_config_path, None)
        logger.info(f"Job config loaded from: {job_config_path}")

        with TaskGroup(task_group_id, dag=dag) as task_group:
            hello_world_task = EmptyOperator(
                dag=dag,
                task_id="hello_world",
                outlets=Asset("dummy://dummy_task_1"),
                doc_yaml="""
                    a: dummy_task_1
                    b: 2
                """,
            )

            bronze_postgre_databricks_etl_task = PythonOperator(
                dag=dag,
                task_id=task_id,
                python_callable=main,
                op_kwargs={
                    "job_config": job_config,
                    "execution_date": "{{ ds }}"
                },
            )
            bronze_postgre_databricks_etl_task.set_upstream(hello_world_task)

            return task_group
