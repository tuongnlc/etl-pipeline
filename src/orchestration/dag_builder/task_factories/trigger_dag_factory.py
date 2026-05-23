from typing import Any
import logging
from src.utils.config_loader import load_and_parse_config
from airflow.sdk import TaskGroup
from src.models.orchestration.task_factories.task_factory import TaskFactoryBase
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TriggerDagsFactory(TaskFactoryBase):
    def _create_task_impl(
            self, 
            task_group_id: str, 
            dag: Any, 
            args: dict[str, Any],
            # child_dags: list[Any]
        ):
            logger.info(f"Create trigger newspaper task groups")

            job_config_path = args["job_config_path"]
            job_config = load_and_parse_config(job_config_path, None)
            logger.info(f"Job config loaded from: {job_config_path}")
            
            child_dags = job_config.childs_dag_ids
            
            with TaskGroup(task_group_id, dag=dag) as task_group:
                previous_task = None
                for trigger_task_id in child_dags:
                    # child_dag.set_upstream(task_group)
                    trigger_task = TriggerDagRunOperator(
                        dag=dag,
                        task_id=f"trigger_{trigger_task_id}",
                        trigger_dag_id=trigger_task_id,
                        wait_for_completion=True,
                        # op_kwargs={"job_config": job_config},
                    )
                
                    if previous_task:
                        previous_task >> trigger_task
                    previous_task = trigger_task
            return task_group
