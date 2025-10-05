{{ config(materialized='table') }}

SELECT 
    DISTINCT to_hex(md5(to_utf8(country))) as countrykey,
    country,
    continent
FROM {{ ref('stg_territories')}}