{{ config(materialized='table')}}

SELECT
    DATE_FORMAT(DATE_PARSE(OrderDate, '%m/%d/%Y'), '%d/%m/%Y') AS OrderDate,
    DATE_FORMAT(DATE_PARSE(StockDate, '%m/%d/%Y'), '%d/%m/%Y') AS stock_date,
    ordernumber,
    productkey,
    customerkey,
    territorykey, 
    orderquantity
FROM {{ ref('sales')}}