import logging
from src.models.orchestration.task_factories.task_factory import TaskFactoryBase
from typing import Any
from airflow.sdk import TaskGroup
from airflow.sdk import Asset
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


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

            task_2 = PythonOperator(
                task_id=task_id,
                python_callable=self._run_task,
                op_kwargs=args,
                provide_context=True,
            )
            task_1 >> task_2
            return task_group
              
