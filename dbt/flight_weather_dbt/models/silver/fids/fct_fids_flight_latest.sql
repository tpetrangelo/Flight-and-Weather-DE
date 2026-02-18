with s as (
    select * 
    from {{ ref('stg_fids_flight_snapshots') }}
),

dedup as (
    select
        s.*,
        row_number() over (
            partition by flight_key
            order by
                loaded_at_ts_utc desc,
                window_end_ts_utc desc,
                source_file desc
        ) as rn
    from s
)

select
    * exclude (rn)
from dedup
where rn = 1