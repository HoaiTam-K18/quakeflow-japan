{{ config(materialized="table") }}

SELECT 
    COALESCE(djp.province_name, 'Offshore') AS province_name,
    COUNT(*) AS earthquake_count
FROM {{ ref("silver_fact_earthquake_event") }} sfe
LEFT JOIN {{ ref("silver_dim_japan_province") }} djp
  ON sfe.province_id = djp.province_id
GROUP BY COALESCE(djp.province_name, 'Offshore')
ORDER BY earthquake_count DESC
