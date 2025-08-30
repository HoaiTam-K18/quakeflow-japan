{{ config(materialized="table") }}

WITH binned AS (
    SELECT 
        CASE
            WHEN magnitude_1 < 3 THEN '<3'
            WHEN magnitude_1 BETWEEN 3 AND 4 THEN '3-4'
            WHEN magnitude_1 BETWEEN 4 AND 5 THEN '4-5'
            WHEN magnitude_1 BETWEEN 5 AND 6 THEN '5-6'
            WHEN magnitude_1 BETWEEN 6 AND 7 THEN '6-7'
            WHEN magnitude_1 >= 7 THEN '>=7'
            ELSE 'Unknown'
        END AS magnitude_range
    FROM {{ ref("silver_fact_earthquake_event") }}
),

counts AS (
    SELECT 
        magnitude_range,
        COUNT(*)::float AS mag_count
    FROM binned
    GROUP BY magnitude_range
),

total AS (
    SELECT COUNT(*)::float AS total_count
    FROM {{ ref("silver_fact_earthquake_event") }}
)

SELECT 
    c.magnitude_range,
    c.mag_count,
    ROUND(((c.mag_count / t.total_count) * 100)::numeric, 2) AS percentage
FROM counts c
CROSS JOIN total t
ORDER BY magnitude_range