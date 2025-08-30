{{ config(materialized="table") }}

WITH binned AS (
    SELECT 
        CASE
            WHEN depth_km < 50 THEN '<50 km'
            WHEN depth_km BETWEEN 50 AND 100 THEN '50-100 km'
            WHEN depth_km BETWEEN 100 AND 200 THEN '100-200 km'
            WHEN depth_km BETWEEN 200 AND 300 THEN '200-300 km'
            WHEN depth_km BETWEEN 300 AND 500 THEN '300-500 km'
            WHEN depth_km >= 500 THEN '>=500 km'
            ELSE 'Unknown'
        END AS depth_range
    FROM {{ ref("silver_fact_earthquake_event") }}
),

counts AS (
    SELECT 
        depth_range,
        COUNT(*)::float AS depth_count
    FROM binned
    GROUP BY depth_range
),

total AS (
    SELECT COUNT(*)::float AS total_count
    FROM {{ ref("silver_fact_earthquake_event") }}
)

SELECT 
    c.depth_range,
    c.depth_count,
    ROUND(((c.depth_count / t.total_count) * 100)::numeric, 2) AS percentage
FROM counts c
CROSS JOIN total t
ORDER BY depth_range
