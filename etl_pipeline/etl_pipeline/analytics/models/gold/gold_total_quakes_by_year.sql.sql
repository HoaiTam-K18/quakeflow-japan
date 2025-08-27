{{ config(materialized="table") }}

SELECT
    EXTRACT(YEAR FROM fee.event_time) AS event_year,
    COUNT(*) AS total_quakes
FROM {{ ref("silver_fact_earthquake_event") }} fee
GROUP BY event_year
ORDER BY event_year
