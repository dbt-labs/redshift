
{{
    config({
        "materialized":"table",
        "sort":"sha",
        "dist":"sha",
        "post-hook": "{{ compress_table('snowplow', 'event') }}"
    })
}}


select * from github.commit
