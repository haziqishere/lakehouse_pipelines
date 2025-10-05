{{ config(materialized='table') }}

SELECT
    to_hex(md5(to_utf(CAST(p.product_key as VARCHAR)))) as productkey,
    p.productname,
    p.productsku,
    p.productcolor,
    ps.subcategoryname,
    pc.category_name

FROM {{ ref("stg_products")}} p
LEFT JOIN {{ ref("stg_product_subcategories")}} ps
    ON p.productsubcategorykey = ps.productsubcategorykey
LEFT JOIN {{ ref("stg_product_categories")}} pc
    ON ps.productcategorykey = pc.productcategorykey