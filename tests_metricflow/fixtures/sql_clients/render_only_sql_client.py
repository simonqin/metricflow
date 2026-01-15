from __future__ import annotations

from typing import Optional

from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_table import SqlTable

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

    def query(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> MetricFlowDataTable:
        raise RuntimeError("Render-only SqlClient does not support query execution.")

    def execute(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> None:
        raise RuntimeError("Render-only SqlClient does not support execution.")

    def dry_run(
        self,
        stmt: str,
        sql_bind_parameter_set: SqlBindParameterSet = SqlBindParameterSet(),
    ) -> None:
        raise RuntimeError("Render-only SqlClient does not support dry_run.")

    def close(self) -> None:
        return None

    def render_bind_parameter_key(self, bind_parameter_key: str) -> str:
        return f":{bind_parameter_key}"

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
