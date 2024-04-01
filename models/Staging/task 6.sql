{{ config(
    materialized = 'incremental',
    unique_key = 'customer_id',
    pre_hook = '{% if is_incremental() %} DELETE FROM {{ this }} WHERE MODIFIED_DATE <> CURRENT_DATE {% endif %}'
) }}

WITH cust AS (
    SELECT *
    FROM {{ source('stg', 'customer') }}
),
Territory AS (
    SELECT *
    FROM {{ source('stg', 'salesterritory') }} 
),
final_cust AS (
    SELECT 
        customer_id::int AS customer_id,
        store_id::int AS store_id,
        account_number::varchar(50) AS account_number,
        c.modified_date::timestamp AS modified_date,
        person_id::int AS person_id,
        c.territory_id::int AS territory_id,
        name::varchar(50) AS name,
        country_region_code::varchar(50) AS country_region_code,
        "group"::varchar(50) AS "group",
        sales_ytd::numeric(38, 9) AS sales_ytd,
        sales_last_year::numeric(38, 9) AS sales_last_year,
        cost_ytd::numeric(38, 9) AS cost_ytd,
        cost_last_year::numeric(38, 9) AS cost_last_year
    FROM cust c
    LEFT JOIN Territory b ON c.TERRITORY_ID = b.TERRITORY_ID 
)
SELECT *
FROM final_cust
WHERE 1=1
{% if is_incremental() %}
    AND MODIFIED_DATE::timestamp > (SELECT MAX(MODIFIED_DATE) FROM {{this}})
{% endif %}
