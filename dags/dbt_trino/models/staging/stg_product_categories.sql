{{ config(materialized='table')}}

SELECT *

FROM {{ reg('product_categories')}}