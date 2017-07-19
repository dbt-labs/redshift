

{{ config(materialized='compressed_table', sort='id', dist='id') }}


select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address
from {{ this.schema }}.people
