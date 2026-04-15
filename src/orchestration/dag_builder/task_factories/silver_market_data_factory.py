import logging
from typing import Any
from src.models.orchestration.task_factories.task_factory import TaskFactoryBase
from airflow.sdk import TaskGroup
from airflow.sdk import Asset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from src.orchestration.airflow.python_script.postgre_to_bq import main
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from src.utils.config_loader import load_and_parse_config


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

            load_to_bq_task = PythonOperator(
                dag=dag,
                task_id=task_id,
                python_callable=main,
                op_kwargs={
                    "job_config": job_config,
                    "execution_date": "{{ ds }}" if job_config.extractor.get('spec').get('execution_date') is not None else None
                }
            )
            # Build task pipeline - using proper Airflow SDK pattern
            # For Airflow SDK, we use conditional task creation with clear branching
            
            if job_config.loader.get('spec').get('enable_delete_before_load') == True:
                # Branch with delete_before_load task
                delete_task = BigQueryInsertJobOperator(
                    dag=dag,
                    task_id="delete_before_load",
                    configuration={
                        "query": {
                            "query": f"""
                                DELETE FROM `{job_config.loader.get('spec').get('dataset')}.{job_config.loader.get('spec').get('table')}` 
                                WHERE DATE(trading_date) >= DATE_ADD(DATE('{{{{ ds }}}}'), INTERVAL -7 DAY)
                            """,
                            "useLegacySql": False,
                        }
                    }
                )
                hello_world_task >> delete_task >> load_to_bq_task
            else:
                # Direct branch without delete task
                hello_world_task >> load_to_bq_task
            
            return task_group
              
