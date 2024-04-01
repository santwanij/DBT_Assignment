{{
  config(materialized='incremental', alias='dim_customer', unique_key='customer_id',
  pre_hook='{% if is_incremental() %} DELETE FROM {{ this }} WHERE MODIFIED_DATE <> CURRENT_DATE {% endif %}')
}}

WITH CTC_customer AS (
    SELECT
        *
    FROM
        {{ source('adventure', 'customer') }}  
),

CTC_salesterritory AS (
    SELECT
        *
    FROM
        {{ source('adventure', 'salesterritory') }}
),

final AS (
    SELECT  
c.customer_id::int,
c.store_id::int,
c.territory_id::int,
c.account_number::varchar(30),
c.rowguid::varchar(30),
c.modified_date::timestamp,
c.person_id::int,
st.name::varchar(30),
st.country_region_code::varchar(30),
st.group::varchar(30), 
st.sales_ytd::numeric(38, 9),
st.sales_last_year::numeric(38, 9),
st.cost_ytd::int,
st.cost_last_year::int 

    FROM 
        CTC_customer c
    INNER JOIN 
        CTC_salesterritory st ON c.Territory_ID = st.Territory_ID
)

SELECT * FROM final
where 1=1

 {% if is_incremental() %}
 and  MODIFIED_DATE::timestamp > (select max(MODIFIED_DATE) from {{this}})
  {% endif %}
