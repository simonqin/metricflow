# StarRocks dbt-metricflow 文档与 SQL 渲染快照设计

## 背景与目标
- 支持 StarRocks 3.x 的 dbt-metricflow 使用路径，补齐接入文档与最小使用示例。
- 新增 StarRocks 的 SQL 渲染快照覆盖，保障渲染输出可回归。
- 不依赖真实 StarRocks 实例，聚焦 SQL 渲染链路。

## 范围
- 文档：新增 `dbt-metricflow/docs/starrocks/README.md`，中文，包含安装、profiles.yml 最小示例、环境变量示例、最小命令示例（`dbt debug`、`dbt run`、`mf query`）。
- 测试：为 StarRocks 生成 SQL 渲染快照，覆盖核心 query rendering 测试用例。

## 非目标
- 不支持 StarRocks 2.x。
- 不新增真实引擎连接或结果快照测试。
- 不修改顶层 `dbt-metricflow/README.md`（不新增入口链接）。

## 设计概述
### 文档内容结构
1) 简介与适用版本（StarRocks 3.x）。
2) 安装：`pip install dbt-metricflow[starrocks]`。
3) `profiles.yml` 最小片段（host/port/user/password/database/schema）。
4) 环境变量示例，用于替换敏感字段。
5) 最小命令示例：`dbt debug`、`dbt run`、`mf query`。

### SQL 渲染快照
- 使用现有 `render_and_check` 流程生成 `SqlPlan` 渲染快照。
- 快照路径归类在 `tests_metricflow/snapshots/.../SqlPlan/StarRocks/`。
- 依赖 `StarRocksSqlPlanRenderer` 与 `SqlEngine.STARROCKS`。

## 关键流程
1) 测试构建 `dataflow plan` 并转换为 `SqlPlan`。
2) 使用 `StarRocksSqlPlanRenderer` 渲染 SQL。
3) 通过快照比对保障渲染结果稳定。

## 错误处理与限制
- 百分位、时间粒度等能力依赖现有渲染器支持范围，超出范围的用例应保持跳过或抛出明确错误。
- 文档强调“SQL 渲染级别验证”，不承诺真实执行兼容性。

## 测试计划
- 生成快照：
  - `hatch run starrocks-env:pytest tests_metricflow/query_rendering/test_query_rendering.py --overwrite-snapshots`
  - `hatch run starrocks-env:pytest tests_metricflow/query_rendering/test_granularity_date_part_rendering.py --overwrite-snapshots`
  - `hatch run starrocks-env:pytest tests_metricflow/query_rendering/test_time_spine_join_rendering.py --overwrite-snapshots`
- 例行校验时去掉 `--overwrite-snapshots`。

## 交付物
- `dbt-metricflow/docs/starrocks/README.md`
- StarRocks SQL 渲染快照文件（位于 `tests_metricflow/snapshots/`）
