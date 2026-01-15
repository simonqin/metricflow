test_name: test_simple_query_with_multiple_date_parts
test_filename: test_granularity_date_part_rendering.py
sql_engine: StarRocks
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT /*+ SET_VAR(cte_inline=true) */
  metric_time__extract_year
  , metric_time__extract_quarter
  , metric_time__extract_month
  , metric_time__extract_day
  , metric_time__extract_dow
  , metric_time__extract_doy
  , SUM(__bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: [
  --   '__bookings',
  --   'metric_time__extract_day',
  --   'metric_time__extract_dow',
  --   'metric_time__extract_doy',
  --   'metric_time__extract_month',
  --   'metric_time__extract_quarter',
  --   'metric_time__extract_year',
  -- ]
  -- Pass Only Elements: [
  --   '__bookings',
  --   'metric_time__extract_day',
  --   'metric_time__extract_dow',
  --   'metric_time__extract_doy',
  --   'metric_time__extract_month',
  --   'metric_time__extract_quarter',
  --   'metric_time__extract_year',
  -- ]
  SELECT /*+ SET_VAR(cte_inline=true) */
    YEAR(ds) AS metric_time__extract_year
    , QUARTER(ds) AS metric_time__extract_quarter
    , MONTH(ds) AS metric_time__extract_month
    , DAY(ds) AS metric_time__extract_day
    , DAY_OF_WEEK_ISO(ds) AS metric_time__extract_dow
    , DAYOFYEAR(ds) AS metric_time__extract_doy
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_9
GROUP BY
  metric_time__extract_year
  , metric_time__extract_quarter
  , metric_time__extract_month
  , metric_time__extract_day
  , metric_time__extract_dow
  , metric_time__extract_doy
