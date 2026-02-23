-- Fail if weather ingestion hasn't produced any new rows recently
{{ config(tags=['freshness']) }}

with recent as (
  select count(*) as rows_last_75m
  from {{ ref('stg_weather_observations') }}
  where loaded_at_ts_utc >= dateadd('minute', -75, current_timestamp())
)
select *
from recent
where rows_last_75m = 0;