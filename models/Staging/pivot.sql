{{ config(alias='Net_sales_pivot') }}

SELECT
    sales_person_id,
    {{ dbt_utils.pivot("year", dbt_utils.get_column_values(ref("yearly_sales_person"), "year"), then_value="currentyearsales") }}
FROM {{ ref('yearly_sales_person') }}
GROUP BY sales_person_id
    