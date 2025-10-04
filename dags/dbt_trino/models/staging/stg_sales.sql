{{ config(materialized='table')}}

SELECT
    DATE_FORMAT(DATE_PARSE(OrderDate, '%m/%d/%Y'), '%d/%m/%Y') AS OrderDate,
    DATE_FORMAT(DATE_PARSE(StockDate, '%m/%d/%Y'), '%d/%m/%Y') AS stock_date,
    order_number,
    productkey,
    customerkey,
    territorykey, 
    order_quantity
FROM {{ ref('sales')}}