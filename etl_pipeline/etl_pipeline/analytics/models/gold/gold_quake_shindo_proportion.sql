{{ config(materialized="table") }}

WITH total AS (
    SELECT COUNT(*)::float AS total_count
    FROM {{ ref("silver_fact_earthquake_event") }}
),

counts AS (
    SELECT 
        shindo_value,
        COUNT(*)::float AS shindo_count
    FROM {{ ref("silver_fact_earthquake_event") }}
    GROUP BY shindo_value
),

percentages AS (
    SELECT 
        c.shindo_value,
        c.shindo_count,
        ROUND(((c.shindo_count / t.total_count) * 100)::numeric, 2) AS shindo_percentage
    FROM counts c
    CROSS JOIN total t
)

SELECT * 
FROM percentages
ORDER BY shindo_value
