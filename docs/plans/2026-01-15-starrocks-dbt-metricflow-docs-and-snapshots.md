# StarRocks dbt-metricflow 文档与快照 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 为 StarRocks 3.x 提供 dbt-metricflow 接入文档，并新增 SQL 渲染快照（不依赖真实引擎）。

**Architecture:** 增加 render-only SqlClient 以绕开 dbt adapter 连接，仅用于渲染 SQL；通过 pytest 生成 StarRocks 渲染快照；新增中文接入文档与最小命令示例。

**Tech Stack:** Python, pytest, hatch, MetricFlow SQL renderers

### Task 1: Render-only SqlClient 与单元测试（@test-driven-development）

**Files:**
- Create: `tests_metricflow/fixtures/sql_clients/render_only_sql_client.py`
- Create: `tests_metricflow/fixtures/test_render_only_sql_client.py`
- Modify: `tests_metricflow/fixtures/sql_client_fixtures.py`

**Step 1: Write the failing test**

```python
from __future__ import annotations

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.starrocks import StarRocksSqlPlanRenderer
from tests_metricflow.fixtures.sql_client_fixtures import make_test_sql_client
from tests_metricflow.fixtures.sql_clients.render_only_sql_client import RenderOnlySqlClient


def test_render_only_sql_client_starrocks(monkeypatch) -> None:
    monkeypatch.setenv("MF_TEST_RENDER_ONLY", "1")
    monkeypatch.setenv("MF_TEST_ADAPTER_TYPE", "starrocks")

    sql_client = make_test_sql_client(
        url="starrocks://user:pass@localhost:9030/db",
        password="dummy",
        schema="dummy_schema",
    )

    assert isinstance(sql_client, RenderOnlySqlClient)
    assert sql_client.sql_engine_type is SqlEngine.STARROCKS
    assert isinstance(sql_client.sql_plan_renderer, StarRocksSqlPlanRenderer)
```

**Step 2: Run test to verify it fails**

Run: `hatch run dev-env:pytest tests_metricflow/fixtures/test_render_only_sql_client.py::test_render_only_sql_client_starrocks -q`
Expected: FAIL (缺少 `RenderOnlySqlClient` 或未启用 render-only 分支)。

**Step 3: Write minimal implementation**

```python
from __future__ import annotations

from typing import Optional

from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet

from metricflow.data_table.mf_table import MetricFlowDataTable
from metricflow.protocols.sql_client import SqlClient, SqlEngine
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderer


class RenderOnlySqlClient(SqlClient):
    def __init__(self, sql_engine_type: SqlEngine, sql_plan_renderer: SqlPlanRenderer) -> None:
        self._sql_engine_type = sql_engine_type
        self._sql_plan_renderer = sql_plan_renderer

    @property
    def sql_engine_type(self) -> SqlEngine:
        return self._sql_engine_type

    @property
    def sql_plan_renderer(self) -> SqlPlanRenderer:
        return self._sql_plan_renderer

    def query(self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()) -> MetricFlowDataTable:
        raise RuntimeError("Render-only SqlClient does not support query execution.")

    def execute(self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()) -> None:
        raise RuntimeError("Render-only SqlClient does not support execution.")

    def dry_run(self, stmt: str, sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet()) -> None:
        raise RuntimeError("Render-only SqlClient does not support dry_run.")

    def close(self) -> None:
        return None

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        return f":{bind_parameter_key}"

    # DDL no-ops for render-only snapshot runs
    def create_table_from_data_table(
        self,
        sql_table: SqlTable,
        df: MetricFlowDataTable,
        chunk_size: Optional[int] = None,
    ) -> None:
        return None

    def create_schema(self, schema_name: str) -> None:
        return None

    def drop_schema(self, schema_name: str, cascade: bool) -> None:
        return None
```

**Step 4: Wire render-only mode in fixtures**

```python
from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import SupportedAdapterTypes
from tests_metricflow.fixtures.sql_clients.render_only_sql_client import RenderOnlySqlClient


def _render_only_sql_client() -> RenderOnlySqlClient:
    adapter_type_value = os.environ.get("MF_TEST_ADAPTER_TYPE")
    if not adapter_type_value:
        raise ValueError("MF_TEST_ADAPTER_TYPE must be set when MF_TEST_RENDER_ONLY=1")
    try:
        adapter_type = SupportedAdapterTypes(adapter_type_value)
    except ValueError as error:
        raise ValueError(f"Unsupported adapter type '{adapter_type_value}' for render-only mode") from error
    return RenderOnlySqlClient(
        sql_engine_type=adapter_type.sql_engine_type,
        sql_plan_renderer=adapter_type.sql_plan_renderer,
    )


def make_test_sql_client(url: str, password: str, schema: str) -> SqlClientWithDDLMethods:
    if os.environ.get("MF_TEST_RENDER_ONLY") == "1":
        return _render_only_sql_client()
    ...
```

**Step 5: Run test to verify it passes**

Run: `hatch run dev-env:pytest tests_metricflow/fixtures/test_render_only_sql_client.py::test_render_only_sql_client_starrocks -q`
Expected: PASS.

**Step 6: Commit**

```bash
git add tests_metricflow/fixtures/sql_clients/render_only_sql_client.py \
  tests_metricflow/fixtures/sql_client_fixtures.py \
  tests_metricflow/fixtures/test_render_only_sql_client.py
git commit -m "Add render-only SQL client for snapshot rendering"
```

### Task 2: StarRocks 文档

**Files:**
- Create: `dbt-metricflow/docs/starrocks/README.md`

**Step 1: Add Chinese docs content**

```markdown
# StarRocks 接入（dbt-metricflow）

## 适用版本
- StarRocks 3.x

## 安装
```bash
pip install "dbt-metricflow[starrocks]"
```

## profiles.yml 最小示例
```yaml
metricflow_testing:
  target: dev
  outputs:
    dev:
      type: starrocks
      host: <host>
      port: 9030
      user: <user>
      password: <password>
      database: <database>
      schema: <schema>
```

## 环境变量示例（可选）
```bash
export DBT_ENV_SECRET_HOST=<host>
export DBT_PROFILE_PORT=9030
export DBT_ENV_SECRET_USER=<user>
export DBT_ENV_SECRET_PASSWORD=<password>
export DBT_ENV_SECRET_DATABASE=<database>
export DBT_ENV_SECRET_SCHEMA=<schema>
```

## 最小命令示例
```bash
dbt debug
dbt run
mf query --metrics transactions --group-by metric_time --order metric_time
```
```

**Step 2: Commit**

```bash
git add dbt-metricflow/docs/starrocks/README.md
git commit -m "Add StarRocks dbt-metricflow docs"
```

### Task 3: 生成 StarRocks SQL 渲染快照

**Files:**
- Update: `tests_metricflow/snapshots/**/SqlPlan/StarRocks/*.sql`

**Step 1: Generate snapshots (query rendering)**

```bash
MF_TEST_RENDER_ONLY=1 \
MF_TEST_ADAPTER_TYPE=starrocks \
MF_SQL_ENGINE_URL=starrocks://user:pass@localhost:9030/db \
/Users/liang/Library/Python/3.9/bin/hatch run dev-env:pytest \
  tests_metricflow/query_rendering/test_query_rendering.py \
  --overwrite-snapshots
```

**Step 2: Generate snapshots (granularity/date part)**

```bash
MF_TEST_RENDER_ONLY=1 \
MF_TEST_ADAPTER_TYPE=starrocks \
MF_SQL_ENGINE_URL=starrocks://user:pass@localhost:9030/db \
/Users/liang/Library/Python/3.9/bin/hatch run dev-env:pytest \
  tests_metricflow/query_rendering/test_granularity_date_part_rendering.py \
  --overwrite-snapshots
```

**Step 3: Generate snapshots (time spine join)**

```bash
MF_TEST_RENDER_ONLY=1 \
MF_TEST_ADAPTER_TYPE=starrocks \
MF_SQL_ENGINE_URL=starrocks://user:pass@localhost:9030/db \
/Users/liang/Library/Python/3.9/bin/hatch run dev-env:pytest \
  tests_metricflow/query_rendering/test_time_spine_join_rendering.py \
  --overwrite-snapshots
```

**Step 4: Commit**

```bash
git add tests_metricflow/snapshots
git commit -m "Add StarRocks SQL rendering snapshots"
```
