with src as (
    select
        *,
        window_start_utc              as window_start_ts_utc,
        window_end_utc                as window_end_ts_utc,
        movement__scheduledtime__utc  as sched_dep_ts_utc,
        movement__revisedtime__utc    as revised_dep_ts_utc,
        movement__runwaytime__utc     as runway_ts_utc
    from {{ source('bronze', 'FLIGHTS') }}
),

clean as (
    select
        upper(trim(departure_airport_iata)) as departure_airport_iata,
        upper(trim(movement__airport__iata)) as arrival_airport_iata,

        replace(upper(trim(number)), ' ', '') as flight_number_clean,

        upper(trim(airline__iata)) as airline_iata,
        upper(trim(airline__icao)) as airline_icao,
        trim(airline__name) as airline_name,

        initcap(trim(status)) as status_raw,
        case
            when lower(trim(status)) in ('expected', 'scheduled') then 'scheduled'
            when lower(trim(status)) in ('delayed') then 'delayed'
            when lower(trim(status)) in ('departed') then 'departed'
            when lower(trim(status)) in ('cancelled', 'canceled') then 'cancelled'
            when lower(trim(status)) in ('arrived') then 'arrived'
            else 'other'
        end as status_normalized,

        try_to_boolean(iscargo) as is_cargo,
        trim(source) as source,
        try_to_number(schema_version) as schema_version,

        window_start_ts_utc,
        window_end_ts_utc,
        sched_dep_ts_utc,
        revised_dep_ts_utc,
        runway_ts_utc,

        coalesce(revised_dep_ts_utc, sched_dep_ts_utc) as dep_best_ts_utc,

        datediff('minute', sched_dep_ts_utc, coalesce(revised_dep_ts_utc, sched_dep_ts_utc)) as dep_delay_min,

        loaded_at as loaded_at_ts_utc,
        source_file,
        row_hash
    from src
),

final as (
    select
        *,
        concat_ws(
            '|',
            departure_airport_iata,
            flight_number_clean,
            to_char(sched_dep_ts_utc, 'YYYYMMDDHH24MI')
        ) as flight_key
    from clean
    where sched_dep_ts_utc is not null
)

select *
from final
qualify row_number() over (
    partition by flight_key, loaded_at_ts_utc
    order by
        loaded_at_ts_utc desc,
        row_hash desc,
        source_file desc
) = 1
