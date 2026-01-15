test_name: test_min_max_only_categorical
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a categorical dimension.
sql_engine: StarRocks
---
-- Write to DataTable
SELECT /*+ SET_VAR(cte_inline=true) */
  subq_3.listing__country_latest__min
  , subq_3.listing__country_latest__max
FROM (
  -- Calculate min and max
  SELECT /*+ SET_VAR(cte_inline=true) */
    MIN(subq_2.listing__country_latest) AS listing__country_latest__min
    , MAX(subq_2.listing__country_latest) AS listing__country_latest__max
  FROM (
    -- Pass Only Elements: ['listing__country_latest']
    SELECT /*+ SET_VAR(cte_inline=true) */
      subq_1.listing__country_latest
    FROM (
      -- Pass Only Elements: ['listing__country_latest']
      SELECT /*+ SET_VAR(cte_inline=true) */
        subq_0.listing__country_latest
      FROM (
        -- Read Elements From Semantic Model 'listings_latest'
        SELECT /*+ SET_VAR(cte_inline=true) */
          1 AS __listings
          , 1 AS __lux_listings
          , listings_latest_src_28000.capacity AS __smallest_listing
          , listings_latest_src_28000.capacity AS __largest_listing
          , 1 AS __active_listings
          , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS ds__day
          , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS ds__week
          , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS ds__month
          , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS ds__quarter
          , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS ds__year
          , YEAR(listings_latest_src_28000.created_at) AS ds__extract_year
          , QUARTER(listings_latest_src_28000.created_at) AS ds__extract_quarter
          , MONTH(listings_latest_src_28000.created_at) AS ds__extract_month
          , DAY(listings_latest_src_28000.created_at) AS ds__extract_day
          , DAY_OF_WEEK_ISO(listings_latest_src_28000.created_at) AS ds__extract_dow
          , DAYOFYEAR(listings_latest_src_28000.created_at) AS ds__extract_doy
          , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS created_at__day
          , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS created_at__week
          , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS created_at__month
          , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS created_at__quarter
          , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS created_at__year
          , YEAR(listings_latest_src_28000.created_at) AS created_at__extract_year
          , QUARTER(listings_latest_src_28000.created_at) AS created_at__extract_quarter
          , MONTH(listings_latest_src_28000.created_at) AS created_at__extract_month
          , DAY(listings_latest_src_28000.created_at) AS created_at__extract_day
          , DAY_OF_WEEK_ISO(listings_latest_src_28000.created_at) AS created_at__extract_dow
          , DAYOFYEAR(listings_latest_src_28000.created_at) AS created_at__extract_doy
          , listings_latest_src_28000.country AS country_latest
          , listings_latest_src_28000.is_lux AS is_lux_latest
          , listings_latest_src_28000.capacity AS capacity_latest
          , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
          , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__ds__week
          , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__ds__month
          , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__ds__quarter
          , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__ds__year
          , YEAR(listings_latest_src_28000.created_at) AS listing__ds__extract_year
          , QUARTER(listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
          , MONTH(listings_latest_src_28000.created_at) AS listing__ds__extract_month
          , DAY(listings_latest_src_28000.created_at) AS listing__ds__extract_day
          , DAY_OF_WEEK_ISO(listings_latest_src_28000.created_at) AS listing__ds__extract_dow
          , DAYOFYEAR(listings_latest_src_28000.created_at) AS listing__ds__extract_doy
          , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
          , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__created_at__week
          , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__created_at__month
          , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__created_at__quarter
          , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__created_at__year
          , YEAR(listings_latest_src_28000.created_at) AS listing__created_at__extract_year
          , QUARTER(listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
          , MONTH(listings_latest_src_28000.created_at) AS listing__created_at__extract_month
          , DAY(listings_latest_src_28000.created_at) AS listing__created_at__extract_day
          , DAY_OF_WEEK_ISO(listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
          , DAYOFYEAR(listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
          , listings_latest_src_28000.country AS listing__country_latest
          , listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , listings_latest_src_28000.capacity AS listing__capacity_latest
          , listings_latest_src_28000.listing_id AS listing
          , listings_latest_src_28000.user_id AS user
          , listings_latest_src_28000.user_id AS listing__user
        FROM ***************************.dim_listings_latest listings_latest_src_28000
      ) subq_0
    ) subq_1
    GROUP BY
      subq_1.listing__country_latest
  ) subq_2
) subq_3
