from typing import Any
import logging
from etl_pipeline.utils.config_loader import load_and_parse_config
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset
from etl_pipeline.orchestration.airflow.python_script.postgre_to_qdrant_bronze import main
from airflow.providers.standard.operators.python import PythonOperator
from etl_pipeline.models.orchestration.task_factories.task_factory import TaskFactoryBase
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BronzeNewspaperTaskFactory(TaskFactoryBase):
    def _create_task_impl(
        self, 
        task_group_id: str, 
        dag: Any, args: dict[str, Any]
    ):
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

            bronze_newspaper_etl_task = PythonOperator(
                dag=dag,
                task_id=task_id,
                python_callable=main,
                op_kwargs={"job_config": job_config},
            )
            bronze_newspaper_etl_task.set_upstream(hello_world_task)

            update_newspaper_postgre_table = SQLExecuteQueryOperator(
                dag=dag,
                task_id="update_newspaper_postgre_table",
                conn_id="postgres_market_data",
                parameters={},
                sql=f"""
                        UPDATE {job_config.extractor.get('spec').get('source_table_name')}
                        SET is_load_to_qdrant = 1
                        WHERE is_load_to_qdrant = 0;
                """,
            )

            update_newspaper_postgre_table.set_upstream(bronze_newspaper_etl_task)
            return task_group