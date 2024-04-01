{{ config(materialized='view', alias='product_category_top_territories_revenue') }}

WITH ranked_territories AS (
    SELECT 
        sh.Territory_ID,
        so.product_id,
        EXTRACT(YEAR FROM sh.Order_Date) AS Year,
        SUM(so.line_total) AS Revenue,
        ROW_NUMBER() OVER (PARTITION BY so.product_id, EXTRACT(YEAR FROM sh.order_Date) ORDER BY SUM(so.line_total) DESC) AS TerritoryRank
     FROM {{ source('stg', 'salesorderheader') }} sh 
     INNER JOIN {{ source('stg', 'salesorderdetail') }} so 
		ON sh.sales_order_id = so.sales_order_id 
    GROUP BY 
        Territory_ID, product_id, EXTRACT(YEAR FROM order_date)
),
final_draft AS (
    SELECT 
        Territory_ID,
        product_id,
        Year,
        Revenue
    FROM 
        ranked_territories
    WHERE 
        TerritoryRank <= 3
    ORDER BY 
        product_id, Year, Revenue DESC
)

SELECT *
FROM final_draft
