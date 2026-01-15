test_name: test_local_dimension_using_local_entity
test_filename: test_query_rendering.py
sql_engine: StarRocks
---
-- Write to DataTable
SELECT /*+ SET_VAR(cte_inline=true) */
  subq_5.listing__country_latest
  , subq_5.listings
FROM (
  -- Compute Metrics via Expressions
  SELECT /*+ SET_VAR(cte_inline=true) */
    subq_4.listing__country_latest
    , subq_4.__listings AS listings
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT /*+ SET_VAR(cte_inline=true) */
      subq_3.listing__country_latest
      , SUM(subq_3.__listings) AS __listings
    FROM (
      -- Pass Only Elements: ['__listings', 'listing__country_latest']
      SELECT /*+ SET_VAR(cte_inline=true) */
        subq_2.listing__country_latest
        , subq_2.__listings
      FROM (
        -- Pass Only Elements: ['__listings', 'listing__country_latest']
        SELECT /*+ SET_VAR(cte_inline=true) */
          subq_1.listing__country_latest
          , subq_1.__listings
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT /*+ SET_VAR(cte_inline=true) */
            subq_0.ds__day
            , subq_0.ds__week
            , subq_0.ds__month
            , subq_0.ds__quarter
            , subq_0.ds__year
            , subq_0.ds__extract_year
            , subq_0.ds__extract_quarter
            , subq_0.ds__extract_month
            , subq_0.ds__extract_day
            , subq_0.ds__extract_dow
            , subq_0.ds__extract_doy
            , subq_0.created_at__day
            , subq_0.created_at__week
            , subq_0.created_at__month
            , subq_0.created_at__quarter
            , subq_0.created_at__year
            , subq_0.created_at__extract_year
            , subq_0.created_at__extract_quarter
            , subq_0.created_at__extract_month
            , subq_0.created_at__extract_day
            , subq_0.created_at__extract_dow
            , subq_0.created_at__extract_doy
            , subq_0.listing__ds__day
            , subq_0.listing__ds__week
            , subq_0.listing__ds__month
            , subq_0.listing__ds__quarter
            , subq_0.listing__ds__year
            , subq_0.listing__ds__extract_year
            , subq_0.listing__ds__extract_quarter
            , subq_0.listing__ds__extract_month
            , subq_0.listing__ds__extract_day
            , subq_0.listing__ds__extract_dow
            , subq_0.listing__ds__extract_doy
            , subq_0.listing__created_at__day
            , subq_0.listing__created_at__week
            , subq_0.listing__created_at__month
            , subq_0.listing__created_at__quarter
            , subq_0.listing__created_at__year
            , subq_0.listing__created_at__extract_year
            , subq_0.listing__created_at__extract_quarter
            , subq_0.listing__created_at__extract_month
            , subq_0.listing__created_at__extract_day
            , subq_0.listing__created_at__extract_dow
            , subq_0.listing__created_at__extract_doy
            , subq_0.ds__day AS metric_time__day
            , subq_0.ds__week AS metric_time__week
            , subq_0.ds__month AS metric_time__month
            , subq_0.ds__quarter AS metric_time__quarter
            , subq_0.ds__year AS metric_time__year
            , subq_0.ds__extract_year AS metric_time__extract_year
            , subq_0.ds__extract_quarter AS metric_time__extract_quarter
            , subq_0.ds__extract_month AS metric_time__extract_month
            , subq_0.ds__extract_day AS metric_time__extract_day
            , subq_0.ds__extract_dow AS metric_time__extract_dow
            , subq_0.ds__extract_doy AS metric_time__extract_doy
            , subq_0.listing
            , subq_0.user
            , subq_0.listing__user
            , subq_0.country_latest
            , subq_0.is_lux_latest
            , subq_0.capacity_latest
            , subq_0.listing__country_latest
            , subq_0.listing__is_lux_latest
            , subq_0.listing__capacity_latest
            , subq_0.__listings
            , subq_0.__lux_listings
            , subq_0.__smallest_listing
            , subq_0.__largest_listing
            , subq_0.__active_listings
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
      ) subq_2
    ) subq_3
    GROUP BY
      subq_3.listing__country_latest
  ) subq_4
) subq_5
