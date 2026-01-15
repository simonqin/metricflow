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
