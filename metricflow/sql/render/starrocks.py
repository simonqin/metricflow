from __future__ import annotations

from typing import Collection, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlExtractExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlSubtractTimeIntervalExpression,
)
from metricflow_semantics.toolkit.string_helpers import mf_indent
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer, SqlPlanRenderResult
from metricflow.sql.render.rendering_constants import SqlRenderingConstants
from metricflow.sql.sql_plan import SqlSelectColumn


class StarRocksSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the StarRocks engine."""

    sql_engine = SqlEngine.STARROCKS

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        """StarRocks supports approximate percentile calculations."""
        return {SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS}

    @property
    @override
    def timestamp_data_type(self) -> str:
        return "DATETIME"

    @override
    def visit_extract_expr(self, node: SqlExtractExpression) -> SqlExpressionRenderResult:
        """Render date part extraction using StarRocks date functions."""
        arg_rendered = self.render_sql_expr(node.arg)

        if node.date_part is DatePart.DOW:
            function_name = "DAY_OF_WEEK_ISO"
        elif node.date_part is DatePart.DOY:
            function_name = "DAYOFYEAR"
        elif node.date_part is DatePart.YEAR:
            function_name = "YEAR"
        elif node.date_part is DatePart.QUARTER:
            function_name = "QUARTER"
        elif node.date_part is DatePart.MONTH:
            function_name = "MONTH"
        elif node.date_part is DatePart.DAY:
            function_name = "DAY"
        else:
            assert_values_exhausted(node.date_part)

        return SqlExpressionRenderResult(
            sql=f"{function_name}({arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time delta operations for StarRocks using DATE_SUB."""
        arg_rendered = node.arg.accept(self)

        count = node.count
        granularity = node.granularity

        # StarRocks uses MySQL-style date arithmetic
        # Convert granularity to INTERVAL keyword
        if granularity is TimeGranularity.NANOSECOND:
            raise UnsupportedEngineFeatureError(
                "Nanosecond granularity is not supported for StarRocks. "
                "Use microsecond granularity or larger."
            )
        elif granularity is TimeGranularity.MICROSECOND:
            interval_unit = "MICROSECOND"
        elif granularity is TimeGranularity.MILLISECOND:
            interval_unit = "MILLISECOND"
        elif granularity is TimeGranularity.SECOND:
            interval_unit = "SECOND"
        elif granularity is TimeGranularity.MINUTE:
            interval_unit = "MINUTE"
        elif granularity is TimeGranularity.HOUR:
            interval_unit = "HOUR"
        elif granularity is TimeGranularity.DAY:
            interval_unit = "DAY"
        elif granularity is TimeGranularity.WEEK:
            interval_unit = "WEEK"
        elif granularity is TimeGranularity.MONTH:
            interval_unit = "MONTH"
        elif granularity is TimeGranularity.QUARTER:
            interval_unit = "QUARTER"
        elif granularity is TimeGranularity.YEAR:
            interval_unit = "YEAR"
        else:
            assert_values_exhausted(granularity)

        return SqlExpressionRenderResult(
            sql=f"DATE_SUB({arg_rendered.sql}, INTERVAL {count} {interval_unit})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
        """Render time delta operations for StarRocks using DATE_ADD."""
        arg_rendered = node.arg.accept(self)

        granularity = node.granularity
        if granularity is TimeGranularity.NANOSECOND:
            raise UnsupportedEngineFeatureError(
                "Nanosecond granularity is not supported for StarRocks. "
                "Use microsecond granularity or larger."
            )
        elif granularity is TimeGranularity.MICROSECOND:
            interval_unit = "MICROSECOND"
        elif granularity is TimeGranularity.MILLISECOND:
            interval_unit = "MILLISECOND"
        elif granularity is TimeGranularity.SECOND:
            interval_unit = "SECOND"
        elif granularity is TimeGranularity.MINUTE:
            interval_unit = "MINUTE"
        elif granularity is TimeGranularity.HOUR:
            interval_unit = "HOUR"
        elif granularity is TimeGranularity.DAY:
            interval_unit = "DAY"
        elif granularity is TimeGranularity.WEEK:
            interval_unit = "WEEK"
        elif granularity is TimeGranularity.MONTH:
            interval_unit = "MONTH"
        elif granularity is TimeGranularity.QUARTER:
            interval_unit = "QUARTER"
        elif granularity is TimeGranularity.YEAR:
            interval_unit = "YEAR"
        else:
            assert_values_exhausted(granularity)

        count_rendered = node.count_expr.accept(self)
        count_sql = (
            f"({count_rendered.sql})" if node.count_expr.requires_parenthesis else count_rendered.sql
        )

        return SqlExpressionRenderResult(
            sql=f"DATE_ADD({arg_rendered.sql}, INTERVAL {count_sql} {interval_unit})",
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
            ),
        )

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        """Render UUID generation for StarRocks."""
        return SqlExpressionRenderResult(
            sql="UUID()",
            bind_parameter_set=SqlBindParameterSet(),
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for StarRocks.
        
        StarRocks supports PERCENTILE_APPROX for approximate percentile calculations.
        """
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameter_set
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            # StarRocks uses PERCENTILE_APPROX(expr, percentile)
            # Note: percentile should be between 0 and 1
            return SqlExpressionRenderResult(
                sql=f"PERCENTILE_APPROX({arg_rendered.sql}, {percentile})",
                bind_parameter_set=params,
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            raise UnsupportedEngineFeatureError(
                "Exact continuous percentile aggregate not supported for StarRocks. Set "
                + "use_approximate_percentile to true in all percentile simple-metrics."
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            raise UnsupportedEngineFeatureError(
                "Discrete percentile aggregate not supported for StarRocks. Set "
                + "use_approximate_percentile to true in all percentile simple-metrics."
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            raise UnsupportedEngineFeatureError(
                "Approximate discrete percentile aggregate not supported for StarRocks. Set "
                + "use_approximate_percentile to true and use continuous percentile in all percentile simple-metrics."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)


class StarRocksSqlPlanRenderer(DefaultSqlPlanRenderer):
    """Plan renderer for the StarRocks engine."""

    EXPR_RENDERER = StarRocksSqlExpressionRenderer()

    @override
    def _render_select_columns_section(
        self,
        select_columns: Sequence[SqlSelectColumn],
        num_parents: int,
        distinct: bool,
    ) -> SqlPlanRenderResult:
        """Render SELECT with StarRocks inline hint to encourage CTE inlining."""
        params = SqlBindParameterSet()
        hint = "/*+ SET_VAR(cte_inline=true) */"
        select_lead = f"SELECT {hint}"
        if distinct:
            select_lead = f"{select_lead} DISTINCT"
        select_section_lines = [select_lead]
        first_column = True
        for select_column in select_columns:
            expr_rendered = self.EXPR_RENDERER.render_sql_expr(select_column.expr)
            params = params.merge(expr_rendered.bind_parameter_set)

            column_select_str = f"{expr_rendered.sql} AS {select_column.column_alias}"
            if num_parents <= 1 and select_column.expr.as_column_reference_expression:
                column_reference = select_column.expr.as_column_reference_expression.col_ref
                if column_reference.column_name == select_column.column_alias:
                    column_select_str = expr_rendered.sql

            if first_column:
                first_column = False
                select_section_lines.append(
                    mf_indent(column_select_str, indent_prefix=SqlRenderingConstants.INDENT)
                )
            else:
                select_section_lines.append(
                    mf_indent(", " + column_select_str, indent_prefix=SqlRenderingConstants.INDENT)
                )

        return SqlPlanRenderResult("\n".join(select_section_lines), params)

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        """Return the StarRocks expression renderer."""
        return self.EXPR_RENDERER
