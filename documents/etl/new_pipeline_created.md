# Hướng dẫn tạo một ETL Job / Pipeline mới

Tài liệu này mô tả kiến trúc repo `etl-pipeline` và các bước cần làm để thêm một job ETL mới.
Nội dung được tổng hợp từ [bronze_postgre_databricks.py](../../etl_pipeline/jobs/bronze_postgre_databricks.py) và các file liên quan.

---

## 1. Kiến trúc tổng quan

Repo chia thành các tầng, mỗi tầng có trách nhiệm riêng. Khi tạo job mới, gần như luôn phải chạm vào nhiều tầng theo đúng thứ tự.

| Tầng | Thư mục | Vai trò |
| --- | --- | --- |
| Templates (abstract) | `etl_pipeline/templates/` | Interface/abstract class: `BasePipeline`, `BaseExtractor`, `BaseLoader`, `BaseTransform`, `TransformStep`. |
| Infrastructure (implementation) | `etl_pipeline/infrastructure/` | Cài đặt cụ thể: extract/load/transform bằng Polars hoặc Spark. |
| Jobs (pipeline) | `etl_pipeline/jobs/` | Ghép extract → transform → load thành một pipeline cụ thể. |
| Models (config) | `etl_pipeline/models/` | Dataclass/Pydantic mô tả cấu hình cho job, extractor, loader, transform, orchestration. |
| Utils | `etl_pipeline/utils/` | `config_loader`, `jobargs`, `airflow_config_loader`… để parse YAML → object. |
| Orchestration | `etl_pipeline/orchestration/` | Airflow (DAG builder, task factories, python_script) và Databricks (bundle jobs, entry points). |
| Configs | `etl_pipeline/configs/` | YAML thật: cấu hình orchestration, cấu hình job, cấu hình Databricks bundle. |

### Luồng chạy khái quát

```
YAML config  ──parse──►  Config dataclass  ──►  Entrypoint (main)  ──►  Pipeline job (.run())
                                                                          │
                                             Extractor ──► Transform ──► Loader
```

Pipeline cơ bản chỉ có 3 hành động: `extract() → transform() → load()`, gói trong `run()`.
Xem interface tại [pipeline/base.py](../../etl_pipeline/templates/pipeline/base.py):

```python
class BasePipeline(ABC):
    @abstractmethod
    def extract(self) -> None: ...
    def transform(self, df, transform_steps): ...
    @abstractmethod
    def load(self) -> None: ...
    def run(self, df, transformation_steps): ...
```

---

## 2. Các loại pipeline hiện có

| Loại | Job class | Entrypoint | Chạy trên | Config job |
| --- | --- | --- | --- | --- |
| Bronze Postgre → Databricks Volume | [bronze_postgre_databricks.py](../../etl_pipeline/jobs/bronze_postgre_databricks.py) | `orchestration/airflow/python_script/postgre_to_databricks_volume_bronze.py` | Airflow | `configs/postgre_to_databricks/**` |
| Bronze Postgre → Qdrant | [bronze_postgre_qdrant.py](../../etl_pipeline/jobs/bronze_postgre_qdrant.py) | `orchestration/airflow/python_script/postgre_to_qdrant_bronze.py` | Airflow | `configs/postgre_to_qdrant/**` |
| Silver Qdrant Embedding | [silver_qdrant_embedding.py](../../etl_pipeline/jobs/silver_qdrant_embedding.py) | `orchestration/airflow/python_script/qrant_to_qdrant_silver.py` | Airflow | `configs/postgre_to_qdrant/**` |
| Volume → Datalake (Silver) | [silver_databrick_lakehouse.py](../../etl_pipeline/jobs/silver_databrick_lakehouse.py) | `orchestration/databricks/voume_to_datalake.py` | Databricks | `configs/databricks/datalake/spark_config/**` |
| Datalake → Datamart (Gold) | [gold_databrick_datamart.py](../../etl_pipeline/jobs/gold_databrick_datamart.py) | `orchestration/databricks/datalake_to_datamart.py` | Databricks | `configs/databricks/data_mart/spark_config/**` |
| Trigger DAG | (không có job class riêng) | dùng `TriggerDagsFactory` | Airflow | `configs/trigger_dag/**` |

---

## 3. Cơ chế config (rất quan trọng, cần nắm trước khi tạo job)

### 3.1 Hai loại YAML

1. **Orchestration YAML** (`configs/orchestration/*.yaml`): `kind: DagDefinition`, mô tả DAG và danh sách task group. Được nạp bởi [central_dag.py](../../etl_pipeline/orchestration/airflow/central_dag.py) qua `ConfigRegistry`.
2. **Job YAML** (`configs/postgre_to_databricks/**`, `configs/databricks/**`): mô tả extractor/transform/loader cụ thể của một job. Được nạp trong task factory (Airflow) hoặc trong entrypoint (Databricks).

### 3.2 `kind` + `spec`

Mỗi YAML dùng cặp `kind` / `spec`. `kind` quyết định class nào sẽ parse `spec`.

```yaml
kind: PostgreToDatabricksVolumeBronzeConfig
spec:
  extractor:
    kind: PostgreDBExtractorWithPolarsConfig     # sub-config cũng có kind/spec riêng
    spec: { ... }
  loader:
    kind: DatabricksVolumeLoaderConfig
    spec: { ... }
```

Việc map `kind` → class nằm ở [config_loader.py](../../etl_pipeline/utils/config_loader.py):

- `CONFIG_PARSER_MAP`: map tên class → dataclass. **Job/extractor/loader config mới phải được thêm vào đây** (hoặc dùng đường dẫn đầy đủ dạng dotted path).
- Nếu `kind` có dấu `.` (ví dụ `etl_pipeline.infrastructure...columns.RemoveColumn`), hệ thống tự import class theo dotted path → dùng cho các transform step.
- Dataclass được parse bằng `dacite.from_dict`; Pydantic model (DagDefinition) thì `model_validate`.

`kind` cho orchestration được map ở [config_registry.py](../../etl_pipeline/orchestration/config/config_registry.py) qua `MODEL_MAPPING`.

---

## 4. Các bước tạo một job mới (Airflow)

Ví dụ xuyên suốt: một job Bronze mới lấy dữ liệu Postgre đẩy lên Databricks Volume (giống `stock_price`).

### Bước 1 — Tạo Pipeline class (`etl_pipeline/jobs/`)

Đây chính là kiểu file như [bronze_postgre_databricks.py](../../etl_pipeline/jobs/bronze_postgre_databricks.py).
Class kế thừa `BasePipeline`, nhận `extractor` + `transformer` + `transform_steps` + `loader` qua `__init__`, và implement `extract/transform/load/run`.

```python
from etl_pipeline.templates.pipeline.base import BasePipeline
from etl_pipeline.templates.etl.extract.postgre_db import PostgreDBExtractor
from etl_pipeline.templates.etl.load.databricks_volume_loader import DatabricksVolumeLoader
from etl_pipeline.templates.etl.transform.base import BaseTransform, TransformStep
import polars as pl


class MyNewPipeline(BasePipeline):
    def __init__(self, extractor, transformer, transform_steps, loader):
        self.extractor = extractor
        self.transformer = transformer
        self.transform_steps = transform_steps
        self.loader = loader

    def extract(self) -> pl.DataFrame:
        return self.extractor.extract()

    def transform(self, data: pl.DataFrame) -> pl.DataFrame:
        df = self.transformer.transform(data, self.transform_steps)
        if isinstance(df, pl.DataFrame):
            return df

    def load(self, transform_data: pl.DataFrame) -> None:
        self.loader.load(transform_data)

    def run(self) -> None:
        data_ = self.extract()
        data_ = self.transform(data_)
        self.load(data_)
```

> **Lưu ý:** ở file `bronze_postgre_databricks.py` hiện tại, dòng 1 đang bị dính comment vào import
> (`# market_data_etl.pyfrom etl_pipeline...`) và `import BigQueryLoader` không dùng tới.
> Khi copy làm template, nhớ tách comment và bỏ import thừa để tránh lỗi cú pháp.

### Bước 2 — Đảm bảo Extractor / Loader / Transform đã có

Kiểm tra tầng `infrastructure/` đã có implementation bạn cần chưa:

- Extractor: [postgre_db.py](../../etl_pipeline/infrastructure/polars/etl/extract/postgre_db.py), `qdrant_extractor.py`, spark `parquet_extractor.py`.
- Loader: [databricks_volume_loader.py](../../etl_pipeline/infrastructure/polars/etl/load/databricks_volume_loader.py), `qdrant_loader.py`, `bq_loader.py`, spark `delta_loader.py`.
- Transform steps: [columns.py](../../etl_pipeline/infrastructure/polars/etl/transform/columns.py) (`SelectColumns`, `AddColumn`, `RemoveColumn`, `RenameColumns`, `ConcatColumns`…), `clean_text.py`, `chunking.py`, `embedding.py`, spark `sql_transform.py`.

Nếu cần loại mới (ví dụ nguồn dữ liệu mới), tạo:
1. Interface trong `templates/etl/{extract|load|transform}/`.
2. Implementation trong `infrastructure/{polars|spark}/etl/...`.
3. Config dataclass trong `models/etl/{extractor|loader|transform}/`.

### Bước 3 — Tạo Job Config dataclass (`etl_pipeline/models/etl/jobs/`)

Mỗi job có một dataclass mô tả config của nó, ví dụ [postgre_to_databricks_volume_bronze.py](../../etl_pipeline/models/etl/jobs/postgre_to_databricks_volume_bronze.py):

```python
from dataclasses import dataclass
from typing import Any, Optional

@dataclass
class MyNewJobConfig:
    loader: Any
    extractor: Any
    transform: Optional[Any] = None
```

Và các config con (nếu cần) đặt ở `models/etl/extractor|loader|transform/`, ví dụ
[postgres_extractor_with_polars.py](../../etl_pipeline/models/etl/extractor/postgres_extractor_with_polars.py),
[databricks_volume_loader.py](../../etl_pipeline/models/etl/loader/databricks_volume_loader.py).

### Bước 4 — Đăng ký config vào `CONFIG_PARSER_MAP`

Thêm class config mới vào map tại [config_loader.py](../../etl_pipeline/utils/config_loader.py):

```python
CONFIG_PARSER_MAP = {
    ...
    MyNewJobConfig.__name__: MyNewJobConfig,
    MyNewExtractorConfig.__name__: MyNewExtractorConfig,
    MyNewLoaderConfig.__name__: MyNewLoaderConfig,
}
```

Nếu không đăng ký, có thể dùng `kind` là dotted path đầy đủ tới class.

### Bước 5 — Tạo Entrypoint (`orchestration/airflow/python_script/`)

Entrypoint `main()` nhận `job_config` (đã parse), dựng extractor/loader/pipeline rồi `run()`.
Xem mẫu [postgre_to_databricks_volume_bronze.py](../../etl_pipeline/orchestration/airflow/python_script/postgre_to_databricks_volume_bronze.py):

```python
def main(job_config=None, execution_date: str = None):
    if job_config.extractor is not None:
        job_config.extractor = parse_config(job_config.extractor)
    if job_config.loader is not None:
        job_config.loader = parse_config(job_config.loader)

    # lấy connection từ Airflow (ví dụ chuỗi kết nối Postgres)
    uri = BaseHook.get_connection("postgres_market_data_polar_uri").password

    extractor = PostgreDBExtractorWithPolars(
        source_table_name=job_config.extractor.source_table_name,
        uri=uri,
        execution_date_filter=execution_date if job_config.extractor.filter_type == "date" else None,
    )
    loader = DatabricksVolumeLoader(
        volume_url=job_config.loader.volume_url,
        local_file_path=job_config.loader.local_file_path,
    )
    MyNewPipeline(extractor=extractor, transformer=QdrantTransform(),
                  transform_steps=[], loader=loader).run()
```

Điểm cần chú ý:
- Dùng `BaseHook.get_connection(...)` cho secret/connection thay vì hardcode.
- `parse_config(...)` để biến các dict sub-config (`kind/spec`) thành object.
- Validate `isinstance` các config trước khi dùng (các file mẫu đều làm vậy).

### Bước 6 — Tạo Task Factory (`orchestration/dag_builder/task_factories/`)

Task factory tạo TaskGroup trong DAG và gọi entrypoint qua `PythonOperator`.
Xem mẫu [bronze_postgre_databricks_factory.py](../../etl_pipeline/orchestration/dag_builder/task_factories/bronze_postgre_databricks_factory.py):

```python
class MyNewFactory(TaskFactoryBase):
    def _create_task_impl(self, task_group_id, dag, args):
        task_id = args["task_id"]
        job_config = load_and_parse_config(args["job_config_path"], None)

        with TaskGroup(task_group_id, dag=dag) as task_group:
            etl_task = PythonOperator(
                dag=dag, task_id=task_id, python_callable=main,
                op_kwargs={"job_config": job_config, "execution_date": "{{ ds }}"},
            )
            return task_group
```

Sau đó **đăng ký factory** trong [central_dag.py](../../etl_pipeline/orchestration/airflow/central_dag.py):

```python
task_factory_registry.register(MyNewFactory())
```

Tên đăng ký mặc định là tên class; `factory_type` trong YAML orchestration phải khớp tên này.

### Bước 7 — Tạo YAML config

**7a. Job YAML** — mô tả extractor/transform/loader, ví dụ
[stock_price.yaml](../../etl_pipeline/configs/postgre_to_databricks/market_data/stock_price.yaml):

```yaml
kind: PostgreToDatabricksVolumeBronzeConfig
spec:
  extractor:
    kind: PostgreDBExtractorWithPolarsConfig
    spec:
      uri: postgresql://postgres:postgres@localhost:5432/market_data
      source_table_name: public.stock_price
      enable_execution_date: true
      filter_type: date
      filter_time_range: 90
  transform:
    kind: QdrantTransformConfig
    spec:
      transform_steps:
        - kind: etl_pipeline.infrastructure.polars.etl.transform.columns.RemoveColumn
          spec:
            column: is_load_to_qdrant
  loader:
    kind: DatabricksVolumeLoaderConfig
    spec:
      volume_url: finance_data_volume.market_data.stock_price
      local_file_path: tmp/stock_price.parquet
```

**7b. Orchestration YAML** — mô tả DAG, ví dụ
[bronze__postgre_to_databricks__stock_price.yaml](../../etl_pipeline/configs/orchestration/bronze__postgre_to_databricks__stock_price.yaml):

```yaml
kind: DagDefinition
metadata:
  name: bronze__postgre_to_databricks__stock_price
spec:
  dag_id: bronze__postgre_to_databricks__stock_price
  schedule: None
  start_date: 2023-01-01
  catchup: False
  default_args:
    owner: tuongnguyen
    retries: 5
  tags: ["Stock Price", "Market Data", "bronze"]
  task_factories:
    - id: job
      factory_type: BronzePostgreDatabricksFactory
      args:
        task_id: "bronze__postgre_to_databricks__stock_price"
        job_config_path: /opt/airflow/dags/etl_pipeline_dags/src/config/postgre_to_databricks/market_data/stock_price.yaml
```

Ràng buộc (được validate trong [airflow_dag.py](../../etl_pipeline/models/orchestration/airflow_dag.py)):
- `dag_id` chỉ gồm `[a-zA-Z0-9_]`.
- `schedule` là preset Airflow (`@daily`…) hoặc `None` hoặc cron 5/6 phần.
- `task_factories[].id` chỉ gồm `[a-zA-Z0-9_]`; `dependencies` phải trỏ tới `id` tồn tại.

---

## 5. Các bước tạo job mới (Databricks bundle)

Với job chạy trên Databricks (Silver/Gold), quy trình khác ở chỗ entrypoint đặt trong
`orchestration/databricks/` và được đóng gói thành wheel.

### Bước 1 — Viết Job class (`etl_pipeline/jobs/`)

Ví dụ [silver_databrick_lakehouse.py](../../etl_pipeline/jobs/silver_databrick_lakehouse.py) (chỉ extract + load, dùng Spark DataFrame) hoặc
[gold_databrick_datamart.py](../../etl_pipeline/jobs/gold_databrick_datamart.py) (chạy SQL datamart).

### Bước 2 — Viết Entrypoint (`orchestration/databricks/`)

Dùng `etl_job_args_utils()` để parse `--job_config`, xem
[voume_to_datalake.py](../../etl_pipeline/orchestration/databricks/voume_to_datalake.py):

```python
def main():
    args = etl_job_args_utils()
    job_config = args.job_config
    if not isinstance(job_config, VolumeToDataLakeConfig):
        raise ValueError("job_config must be of type VolumeToDataLakeConfig")

    spark = SparkSession.builder.getOrCreate()
    extractor = ParquetExtractorWithSpark(parquet_uri=job_config.extractor.parquet_uri, spark=spark)
    loader = DeltaLoaderWithSpark(delta_table_name=job_config.loader.delta_table_name, spark=spark)
    SilverDatabrickLakehouseJob(extractor=extractor, loader=loader).run()
```

### Bước 3 — Đăng ký entry point trong `pyproject.toml`

[pyproject.toml](../../pyproject.toml) mục `[project.scripts]`:

```toml
[project.scripts]
my_new_job = "etl_pipeline.orchestration.databricks.my_new_job:main"
```

`entry_point` này phải khớp với job YAML của bundle.

### Bước 4 — Tạo Job YAML cho bundle (`configs/databricks/**`)

Mỗi job là một file khai báo `resources.jobs`, ví dụ
[stock_price.yaml](../../etl_pipeline/configs/databricks/datalake/job_config/stock_price.yaml):

```yaml
resources:
  jobs:
    volume_to_datalake__stock_price:
      name: volume_to_datalake__stock_price
      parameters:
        - name: job_config
          default: ${workspace.file_path}/configs/databricks/datalake/spark_config/stock_price.yaml
      tasks:
        - task_key: volume_to_datalake__stock_price
          python_wheel_task:
            package_name: etl_pipeline            # khớp [project].name
            entry_point: volume_to_datalake        # khớp [project.scripts]
            parameters:
              - "--job_config"
              - "${workspace.file_path}/configs/databricks/datalake/spark_config/stock_price.yaml"
          environment_key: default
      environments:
        - environment_key: default
          spec:
            environment_version: "5"
            dependencies:
              - ../../../../dist/*.whl
              - dacite==1.9.2
```

### Bước 5 — Tạo config nội dung job (`configs/databricks/**/spark_config/`)

Ví dụ [stock_price.yaml](../../etl_pipeline/configs/databricks/datalake/spark_config/stock_price.yaml):

```yaml
kind: VolumeToDataLakeConfig
spec:
  extractor:
    kind: ParquetExtractorWithSparkConfig
    spec: { catalog: finance_data_volume, schema: market_data, volume: stock_price }
  loader:
    kind: DeltaLoaderWithSparkConfig
    spec: { catalog: finance_datalake, schema: market_data, table: stock_price }
```

### Bước 6 — Khai báo vào `databricks.yml`

[databricks.yml](../../etl_pipeline/databricks.yml) include các thư mục job YAML:

```yaml
include:
  - configs/databricks/datalake/job_config/*.yaml
  - configs/databricks/data_mart/job_config/**/*.yaml
  ...
```

Thêm đường dẫn tương ứng nếu bạn tạo nhóm job mới. `sync.include` cần chứa thư mục config
để bundle đẩy file lên workspace.

---

## 6. Tạo job Trigger DAG

Dùng khi cần một DAG cha kích hoạt các DAG con.

1. Config dataclass [trigger_dag.py](../../etl_pipeline/models/etl/jobs/trigger_dag.py) đã có (`childs_dag_ids`).
2. Factory [trigger_dag_factory.py](../../etl_pipeline/orchestration/dag_builder/task_factories/trigger_dag_factory.py) dùng `TriggerDagRunOperator` — đã đăng ký sẵn trong `central_dag.py`.
3. Config job [trigger_stock_price.yaml](../../etl_pipeline/configs/trigger_dag/trigger_stock_price.yaml):

```yaml
kind: TriggerDagJobConfig
spec:
  childs_dag_ids:
    - 2026_06_09_stock_price_crawler
    - bronze__postgre_to_databricks__stock_price
```

4. Orchestration YAML `configs/orchestration/trigger_*.yaml` với `factory_type: TriggerDagsFactory`.

Nếu chỉ muốn trigger các DAG có sẵn, không cần viết code mới — chỉ cần thêm YAML config.

---

## 7. Checklist tạo job mới

**Job Airflow (Bronze):**
- [ ] `etl_pipeline/jobs/<pipeline>.py` — pipeline class kế thừa `BasePipeline`.
- [ ] `etl_pipeline/models/etl/jobs/<job>_config.py` — dataclass config job.
- [ ] (nếu mới) config + implementation cho extractor/loader/transform.
- [ ] Đăng ký class vào `CONFIG_PARSER_MAP` trong `utils/config_loader.py`.
- [ ] `orchestration/airflow/python_script/<entry>.py` — entrypoint `main()`.
- [ ] `orchestration/dag_builder/task_factories/<factory>.py` — task factory.
- [ ] Đăng ký factory trong `orchestration/airflow/central_dag.py`.
- [ ] `configs/<nhóm>/<job>.yaml` — job config.
- [ ] `configs/orchestration/<dag>.yaml` — orchestration DAG config.

**Job Databricks (Silver/Gold):**
- [ ] `etl_pipeline/jobs/<pipeline>.py` — job class.
- [ ] `orchestration/databricks/<entry>.py` — entrypoint dùng `etl_job_args_utils()`.
- [ ] `pyproject.toml` — thêm entry point vào `[project.scripts]`.
- [ ] `configs/databricks/<layer>/job_config/<job>.yaml` — bundle job definition.
- [ ] `configs/databricks/<layer>/spark_config/<job>.yaml` — nội dung job config.
- [ ] `databricks.yml` — thêm `include` nếu tạo nhóm job mới.
- [ ] (nếu cần) đăng ký config class vào `CONFIG_PARSER_MAP`.

**Trigger:**
- [ ] `configs/trigger_dag/<job>.yaml` — danh sách `childs_dag_ids`.
- [ ] `configs/orchestration/trigger_<job>.yaml` — DAG với `TriggerDagsFactory`.

---

## 8. Ghi chú & quy ước

- **Một chiều phụ thuộc:** `templates` ← `infrastructure` ← `jobs` ← `orchestration`; `models` và `configs` được dùng xuyên suốt nhưng không import ngược lại orchestration.
- **Đừng hardcode secret** trong config; dùng Airflow Connection (`BaseHook.get_connection`) hoặc biến môi trường (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`).
- **`job_config_path` trong orchestration YAML** là đường dẫn tuyệt đối trên môi trường Airflow (`/opt/airflow/dags/...`), khác với job config trong repo.
- **`kind` phải trùng khớp** giữa YAML và `CONFIG_PARSER_MAP` (hoặc dotted path), nếu không sẽ lỗi `Unknown config kind`.
- **Transform step** dùng `kind` là dotted path đầy đủ tới class (`etl_pipeline.infrastructure...`), không cần đăng ký map.
- Tên `factory_type` trong orchestration YAML phải trùng tên class đã `register(...)` (hoặc tên tùy chọn truyền vào tham số thứ hai của `register`).
