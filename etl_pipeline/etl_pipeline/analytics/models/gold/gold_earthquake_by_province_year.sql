{{ config(materialized="table") }}

WITH source_data AS (
    SELECT 
        COALESCE(djp.province_name, 'Offshore') AS province_name,
        EXTRACT(YEAR FROM fee.event_time)::int AS event_year
    FROM {{ ref("silver_fact_earthquake_event") }} fee
    LEFT JOIN {{ ref("silver_dim_japan_province") }} djp
      ON fee.province_id = djp.province_id
)

SELECT 
    province_name,
    {{
        dbt_utils.pivot(
            column='event_year',
            values=[2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,
                    2010,2011,2012,2013,2014,2015,2016,2017,2018,2019],
            agg='sum',
            then_value='1'
        )
    }}
FROM source_data
GROUP BY province_name
ORDER BY province_name
