from __future__ import annotations

from typing import Collection

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlArithmeticExpression,
    SqlArithmeticOperator,
    SqlExtractExpression,
    SqlGenerateUuidExpression,
    SqlIntegerExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlSubtractTimeIntervalExpression,
)
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer


class KylinSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Apache Kylin engine.
    
    Kylin uses Spark SQL as its query engine, so the syntax is similar to Databricks/Spark.
    """

    sql_engine = SqlEngine.KYLIN

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        """Kylin supports approximate percentile calculations via PERCENTILE_APPROX."""
        return {SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS, SqlPercentileFunctionType.APPROXIMATE_DISCRETE}

    @override
    def render_date_part(self, date_part: DatePart) -> str:
        """Render date part for Kylin (Spark SQL).
        
        Kylin supports EXTRACT(DOW FROM ...) for day of week.
        """
        if date_part is DatePart.DOW:
            return "DOW"
        
        return super().render_date_part(date_part)

    @override
    def visit_extract_expr(self, node: SqlExtractExpression) -> SqlExpressionRenderResult:
        """Render EXTRACT expressions with ISO day-of-week normalization."""
        extract_rendering_result = super().visit_extract_expr(node)

        if node.date_part is not DatePart.DOW:
            return extract_rendering_result

        extract_stmt = extract_rendering_result.sql
        case_expr = f"CASE WHEN {extract_stmt} = 1 THEN 7 ELSE {extract_stmt} - 1 END"

        return SqlExpressionRenderResult(
            sql=case_expr,
            bind_parameter_set=extract_rendering_result.bind_parameter_set,
        )

    @override
    def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time delta operations for Kylin using Spark SQL INTERVAL syntax.
        
        Kylin uses Spark SQL, which supports INTERVAL expressions like:
        timestamp - INTERVAL 1 DAY
        """
        arg_rendered = node.arg.accept(self)
        
        count = node.count
        granularity = node.granularity
        
        # Validate granularity support
        if granularity is TimeGranularity.NANOSECOND:
            raise UnsupportedEngineFeatureError(
                "Nanosecond granularity is not supported for Kylin. "
                "Use millisecond granularity or larger."
            )
        elif granularity is TimeGranularity.MICROSECOND:
            raise UnsupportedEngineFeatureError(
                "Microsecond granularity is not supported for Kylin. "
                "Use millisecond granularity or larger."
            )
        
        # Map granularity to Spark SQL INTERVAL unit
        # Spark SQL supports: MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR
        if granularity is TimeGranularity.QUARTER:
            # Spark SQL doesn't have QUARTER, convert to months
            interval_unit = "MONTH"
            count *= 3
        else:
            interval_unit = granularity.value.upper()
        
        # Use Spark SQL INTERVAL syntax
        return SqlExpressionRenderResult(
            sql=f"{arg_rendered.sql} - INTERVAL {count} {interval_unit}",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
        """Render time delta operations for Kylin using Spark SQL INTERVAL syntax."""
        arg_rendered = node.arg.accept(self)

        granularity = node.granularity
        count_expr = node.count_expr

        # Validate granularity support
        if granularity is TimeGranularity.NANOSECOND:
            raise UnsupportedEngineFeatureError(
                "Nanosecond granularity is not supported for Kylin. "
                "Use millisecond granularity or larger."
            )
        elif granularity is TimeGranularity.MICROSECOND:
            raise UnsupportedEngineFeatureError(
                "Microsecond granularity is not supported for Kylin. "
                "Use millisecond granularity or larger."
            )

        # Map granularity to Spark SQL INTERVAL unit
        if granularity is TimeGranularity.QUARTER:
            interval_unit = "MONTH"
            count_expr = SqlArithmeticExpression.create(
                left_expr=node.count_expr,
                operator=SqlArithmeticOperator.MULTIPLY,
                right_expr=SqlIntegerExpression.create(3),
            )
        else:
            interval_unit = granularity.value.upper()

        count_rendered = count_expr.accept(self)
        count_sql = f"({count_rendered.sql})" if count_expr.requires_parenthesis else count_rendered.sql

        return SqlExpressionRenderResult(
            sql=f"{arg_rendered.sql} + INTERVAL {count_sql} {interval_unit}",
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
            ),
        )

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        """Render UUID generation for Kylin (Spark SQL).
        
        Spark SQL has a uuid() function that generates a random UUID.
        """
        return SqlExpressionRenderResult(
            sql="UUID()",
            bind_parameter_set=SqlBindParameterSet(),
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for Kylin using PERCENTILE_APPROX."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameter_set
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type in (
            SqlPercentileFunctionType.APPROXIMATE_DISCRETE,
            SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
        ):
            # Kylin uses PERCENTILE_APPROX(expr, percentile)
            return SqlExpressionRenderResult(
                sql=f"PERCENTILE_APPROX({arg_rendered.sql}, {percentile})",
                bind_parameter_set=params,
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            raise UnsupportedEngineFeatureError(
                "Exact continuous percentile aggregate not supported for Kylin. Use "
                + "approximate percentile in all percentile simple-metrics."
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            raise UnsupportedEngineFeatureError(
                "Discrete percentile aggregate not supported for Kylin. Use "
                + "approximate percentile in all percentile simple-metrics."
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)


class KylinSqlPlanRenderer(DefaultSqlPlanRenderer):
    """Plan renderer for the Apache Kylin engine."""

    EXPR_RENDERER = KylinSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        """Return the Kylin expression renderer."""
        return self.EXPR_RENDERER
