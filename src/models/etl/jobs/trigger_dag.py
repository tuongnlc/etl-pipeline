from dataclasses import dataclass

@dataclass
class TriggerDagJobConfig:
    childs_dag_ids: list[str]
