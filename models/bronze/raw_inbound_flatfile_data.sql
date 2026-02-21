{{ config(materialized='view') }}

SELECT
  flatfile_name
  ,app_id
  ,flatfile_content
  ,bq_insert_date
FROM `xomnia-steam.raw_inbound.flatfile_data`