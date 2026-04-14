import logging
from src.models.orchestration.task_factories.task_factory import TaskFactoryBase
from typing import Any
from airflow.sdk import TaskGroup
from airflow.sdk import Asset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from src.orchestration.airflow.python_script.postgre_to_bq import main
# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

logger = logging.getLogger(__name__)


class SilverMarketDataTaskFactory(TaskFactoryBase):
    """
        Silver market data task factory.
    """
    from src.models.orchestration.task_factories.silver_market_data_factory import SilverMarketDataTaskModel
    model_class = SilverMarketDataTaskModel

    def _create_task_impl(
        self,
        task_group_id: str, dag: Any, args: dict[str, Any]
    ):
        """
            Create the task.
        """
        task_id = args["task_id"]
        logger.info(f"Create silver market data task groups")

        with TaskGroup(task_group_id, dag=dag) as task_group:
            task_1 = EmptyOperator(
                dag=dag,
                task_id="hello_world",
                outlets=Asset("dummy://dummy_task_1"),
                doc_yaml="""
                    a: dummy_task_1
                    b: 2
                """,
            )

            # task_2 = BigQueryExecuteQueryOperator(
            #     dag=dag,
            #     task_id="delete_before_load",
            #     query="SELECT 1 AS a",
            #     project="rich-finance-2026",
            #     use_legacy_sql=False,
            # )

            task_3 = PythonOperator(
                dag=dag,
                task_id=task_id,
                python_callable=main,
                op_kwargs={
                    "job_config_path": args["job_config_path"]
                }
            )
            task_1 >> task_3
            return task_group
              
