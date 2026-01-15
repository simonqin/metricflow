test_name: test_distinct_values
test_filename: test_query_rendering.py
docstring:
  Tests a plan to get distinct values for a dimension.
sql_engine: StarRocks
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listing__country_latest']
-- Order By ['listing__country_latest'] Limit 100
-- Write to DataTable
SELECT /*+ SET_VAR(cte_inline=true) */
  listing__country_latest
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Pass Only Elements: ['listing__country_latest']
  SELECT /*+ SET_VAR(cte_inline=true) */
    country AS listing__country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_6
WHERE listing__country_latest = 'us'
GROUP BY
  listing__country_latest
ORDER BY listing__country_latest DESC
LIMIT 100
