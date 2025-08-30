{{ config(materialized="table") }}

WITH monthly AS (
    SELECT 
        EXTRACT(MONTH FROM event_time)::int AS event_month,
        COUNT(*) AS earthquake_count
    FROM {{ ref("silver_fact_earthquake_event") }}
    GROUP BY event_month
),

total AS (
    SELECT SUM(earthquake_count) AS total_count
    FROM monthly
)

SELECT 
    m.event_month,
    m.earthquake_count,
    ROUND(100.0 * m.earthquake_count / t.total_count, 2) AS percentage
FROM monthly m
CROSS JOIN total t
ORDER BY m.event_month
