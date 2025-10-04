{{ config(materialized='table')}}

SELECT
    s.orderdate,
    s.ordernumber,
    to_hex(md5(to_utf8(CAST(s.productkey)))) as productkey,
    to_hext(md5(to_utf8(t.country))) as countrykey,
    (s.order_quantity * p.product_price) as revenue,
    (s.order_quantity * p.product_cost) as cost,
    (s.order_quantity * p.product_price) - (s.order_quantity * p.product_cost) as profit

FROM {{ ref('stg_sales')}} s
LEFT JOIN {{ ref('dim_products')}} p
    ON s.productkey = p.productkey
LEFT JOIN {{ ref('stg_territories')}} t
    ON s.territorykey = t.territorykey