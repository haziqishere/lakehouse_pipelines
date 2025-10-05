{{ config(materialized='table')}}

SELECT
    s.orderdate AS order_date, 
    s.ordernumber AS order_number,
    to_hex(md5(to_utf8(CAST(s.productkey AS VARCHAR)))) as product_key,
    to_hex(md5(to_utf8(t.country))) as country_key,
    (s.orderquantity * p.productprice) as revenue,
    (s.orderquantity * p.productcost) as cost,
    (s.orderquantity * p.productprice) - (s.orderquantity * p.productcost) as profit

FROM {{ ref('stg_sales')}} s
LEFT JOIN {{ ref('stg_products')}} p
    ON s.productkey = p.productkey
LEFT JOIN {{ ref('stg_territories')}} t
    ON s.territorykey = t.salesterritorykey