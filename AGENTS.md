# MetricFlow Project Rules

This project uses **hatch** for dependency management and testing:

- Dependencies: Use `hatch` commands, not `pip install` directly.
- Linting: Run `make lint` to detect and fix lint errors.
- Testing:
  - Use `hatch run dev-env:pytest <path>` to run tests in a specific file.
  - Use `make test` to run all tests.
- Snapshots: When adding new tests that use snapshots, add the `--overwrite-snapshots`
  flag to the above `pytest` command to generate snapshots.

If `git_ignored/AGENTS.md` exists, append the rules in that file.

## Python Code Standards

- **Always add type annotations** to all Python functions (parameters and return types)
- Use `from __future__ import annotations` at the top of Python files

# Repository Guidelines

## 项目结构与模块组织
- 核心库在`metricflow/`（数据流、SQL 生成、执行、验证等子模块）；语义层独立于`metricflow-semantics/metricflow_semantics/`，并通过`pyproject.toml`一起打包。
- `dbt-metricflow/`提供与dbt集成的包装与适配代码；命令行和脚本位于`scripts/`。
- 测试按层次分布：核心测试在`tests_metricflow/`，语义层测试在`metricflow-semantics/tests_metricflow_semantics/`，性能测试位于`tests_metricflow/performance/`（默认忽略）。
- 依赖与环境定义集中在`pyproject.toml`与`requirements-files/`，静态检查配置在`ruff.toml`与`mypy.ini`。

## 构建、测试与开发命令
- 安装开发工具：`make install-hatch`。
- 运行核心与语义层测试（DuckDB 默认）：`make test`；包含slow测试：`make test-include-slow`。
- 引擎专项测试：`make test-<engine>`（如`make test-postgresql`）；需要设置`MF_SQL_ENGINE_URL`、`MF_SQL_ENGINE_PASSWORD`与对应适配器凭据。
- 选择性测试示例：`hatch run dev-env:pytest tests_metricflow/plan_conversion/test_dataflow_to_sql_plan.py -k query`。
- 代码质量：`make lint`（pre-commit集成Black、Ruff、Mypy）；性能基准与对比：`make perf`，`make perf-compare A=<old.json> B=<new.json>`。
- 其他常用命令：`make regenerate-test-snapshots`更新快照，`make sync-dsi`同步语义接口镜像。

## 代码风格与命名约定
- 使用Black格式化，行宽120；Ruff启用E/F/D/I规则，docstring遵循Google风格，`from __future__ import annotations`应作为首个导入。
- Mypy强类型：禁止未注解的定义与显式`Any`；遵循`mypy.ini`中的忽略列表。
- Python文件、包、测试名使用`snake_case`；测试函数以`test_*`或`populate_source_schema*`开头。
- 保持小而聚焦的变更，必要时在复杂逻辑处添加简短注释。

## 测试指南
- 默认命令跳过`tests_metricflow/performance/`；需要性能数据时使用`make perf`。
- 覆盖新增逻辑的单测或集成用例，若涉及SQL渲染，可在`tests_metricflow/integration/test_cases/`添加/调整YAML；更新快照后记得提交。
- 运行跨引擎测试前，确保目标仓库可读写并导出适配环境变量；DuckDB本地为最快反馈路径。

## 提交与拉取请求
- Commit 信息：标题简洁（建议祈使句），主体说明动机与主要改动；一次commit聚焦单一概念性变更。
- PR 要求：描述问题与解决方案、列出已运行的命令/测试、关联Issue或需求；涉及用户可见变化时附示例或截图。
- 更新变更日志时使用`changie new`生成条目，不直接修改`CHANGELOG.md`。
