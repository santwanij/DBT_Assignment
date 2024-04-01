{{ config(materialized='view') }}

-- Use a Common Table Expression (CTE) to define your data transformation
with product_flag as (
    select 
*,
    CASE 
        WHEN Make_flag = 't' AND (Size IS NULL OR Size = '') THEN 1 ELSE 0 
    END AS is_make_no_size,
    CASE WHEN Finished_Goods_Flag = 't' AND Weight > 25 THEN 1 ELSE 0 END AS is_finished_and_over_25
   
    FROM 
          {{source('adventure','product')}} 
)
, 
product as (
    select 
     *,
     case when is_make_no_size = 1 or is_finished_and_over_25 = 1 then 1 else 0 end as is_rel
FROM  product_flag),

productsubcategory as (
select *
from {{source('adventure','product_sub_category')}}
),
productcategory AS (
select *
from {{source('adventure','product_category')}}
),

final_product AS (
select 
product_id
,p.name 
,product_number 
,MAKE_FLAG 
,is_make_no_size 
,finished_goods_flag 
,is_finished_and_over_25 
,is_rel 
,safety_stock_level 
,reorder_point 
,standard_cost 
,list_price 
,days_to_manufacture 
,sell_start_date 
,p.modified_date 
,color 
,"class" 
,weight_unit_measure_code 
,weight 
,size 
,size_unit_measure_code 
,product_line 
,style 
,product_model_id 
,sell_end_date 
,pc.product_category_id 
,pc.name  as Category_name
,ps.product_subcategory_id 
,ps.name as Subcategory_name 
from product p
inner join productsubcategory ps ON ps.product_subcategory_id = p.product_subcategory_id
inner join productcategory pc ON pc.product_category_id = ps.product_category_id
)

select *
from final_product
