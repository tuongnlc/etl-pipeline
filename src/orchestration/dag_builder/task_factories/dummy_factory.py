import logging
from src.models.orchestration.task_factories.task_factory import TaskFactoryBase
from typing import Any
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset


logger = logging.getLogger(__name__)


class DummyTaskFactory(TaskFactoryBase):
    """
        Dummy task factory to be used for testing.
    """
    from src.models.orchestration.task_factories.dummy_factory import DummyTaskModel
    model_class = DummyTaskModel

    def _create_task_impl(
        self,
        task_group_id: str, dag: Any, args: dict[str, Any]
    ):
        """
            Create a task group for the given task group ID
        """
        logger.info(f"Create task group: {task_group_id}")

        if "custom_dummy_arg1" in args:
            logger.debug(f"custom_dummy_arg1: {args['custom_dummy_arg1']}")
        if "custom_dummy_arg2" in args:
            logger.debug(f"custom_dummy_arg2: {args['custom_dummy_arg2']}")
        with TaskGroup(dag=dag, group_id=task_group_id) as task_group:
            task_1 = EmptyOperator(
                dag=dag,
                task_id="task_1",
                outlets=Asset("dummy://dummy_task_1"),
                doc_yaml="""
                    a: dummy_task_1
                    b: 2
                """,
            )

            task_2 = EmptyOperator(
                dag=dag,
                task_id="task_2",
                outlets=Asset("dummy://dummy_task_2"),
                doc_yaml="""
                    a: dummy_task_2
                    b: 2
                """
            )
            task_1 >> task_2
            return task_group
           
