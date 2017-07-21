

{{ config(materialized='compressed_table', sort='id', dist='id', comprows=1000000) }}

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address
from {{ this.schema }}.people
