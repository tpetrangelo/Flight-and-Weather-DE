with w as (
    select *
    from {{ ref('stg_weather_observations') }}
),

dedup as (
    select
        w.*,
        row_number() over (
            partition by airport_iata
            order by
                obs_ts_utc desc,
                loaded_at_ts_utc desc,
                row_hash desc,
                source_file desc
        ) as rn
    from w
)

select
    * exclude (rn)
from dedup
where rn = 1