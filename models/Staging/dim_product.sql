{{
  config(materialized='incremental', alias='dim_product', unique_key='product_id',
  pre_hook='{% if is_incremental() %} DELETE FROM {{ this }} WHERE MODIFIED_DATE <> CURRENT_DATE {% endif %}')
}}

--
-- Use a Common Table Expression (CTE) to define your data transformation
with product_flag as (
    select 
*,
    CASE 
        WHEN Make_flag = true AND (Size IS NULL OR Size = '') THEN 1 ELSE 0 
    END AS is_make_no_size,
    CASE WHEN Finished_Goods_Flag = true AND Weight > 25 THEN 1 ELSE 0 END AS is_finished_and_over_25
   
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
product_id::int
,p.name::varchar(30) as Product_name 
,product_number::varchar(30)
,MAKE_FLAG::bool
,is_make_no_size::int
,finished_goods_flag::bool
,is_finished_and_over_25::int
,is_rel::int
,safety_stock_level::int
,reorder_point::int
,standard_cost::numeric(38, 9)
,list_price::numeric(38, 9)
,days_to_manufacture:: int
,sell_start_date::timestamp
,p.modified_date::timestamp
,color::varchar(30)
,class::varchar(30)
,weight_unit_measure_code::varchar(30)
,weight::numeric(38, 9)
,size::varchar(30)
,size_unit_measure_code::varchar(30)
,product_line::varchar(30)
,style::varchar(30)
,product_model_id::int
,sell_end_date::timestamp
,pc.product_category_id::int
,pc.name::varchar(30) as Category_name
,ps.product_subcategory_id::int
,ps.name::varchar(30) as Subcategory_name 
from product p
inner join productsubcategory ps ON ps.product_subcategory_id = p.product_subcategory_id
inner join productcategory pc ON pc.product_category_id = ps.product_category_id
)

select *
from final_product
 where 1=1

 {% if is_incremental() %}
 and  MODIFIED_DATE::timestamp > (select max(MODIFIED_DATE) from {{this}})
  {% endif %}]
