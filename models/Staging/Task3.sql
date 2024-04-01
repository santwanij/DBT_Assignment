-- models/task3.sql

-- This model moves data from the staging schema to the data warehouse schema and adds the etl_time column

-- Create the model with the appropriate schema name
{{ config(
    materialized='table',
    schema='dwh'
) }}

-- Select data from the staging table and add etl_time column
select
    *,
    getdate() as etl_time
from
    stg.your_staging_table;
