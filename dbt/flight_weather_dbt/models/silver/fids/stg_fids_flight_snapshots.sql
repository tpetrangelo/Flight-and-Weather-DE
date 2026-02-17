with src as (
    select
        *,
        -- Parse timestamps (Snowflake will accept many formats, TRY_ is safer)
        try_to_timestamp_ntz(window_start_utc)               as window_start_ts_utc,
        try_to_timestamp_ntz(window_end_utc)                 as window_end_ts_utc,
        try_to_timestamp_ntz(movement__scheduledtime__utc)   as sched_dep_ts_utc,
        try_to_timestamp_ntz(movement__revisedtime__utc)     as revised_dep_ts_utc,
        try_to_timestamp_ntz(movement__runwaytime__utc)      as runway_ts_utc
    from {{ source('bronze', 'FLIGHTS') }}

),

clean as (
    select
        -- cleaned identifiers
        upper(trim(departure_airport_iata)) as departure_airport_iata,
        upper(trim(movement__airport__iata)) as arrival_airport_iata,

        -- normalize flight number: "DL 5654" -> "DL5654"
        replace(upper(trim(number)), ' ', '') as flight_number_clean,

        upper(trim(airline__iata)) as airline_iata,
        upper(trim(airline__icao)) as airline_icao,
        trim(airline__name) as airline_name,

        -- status normalization (basic)
        initcap(trim(status)) as status_raw,
        case
            when lower(trim(status)) in ('expected', 'scheduled') then 'scheduled'
            when lower(trim(status)) in ('delayed') then 'delayed'
            when lower(trim(status)) in ('departed') then 'departed'
            when lower(trim(status)) in ('cancelled', 'canceled') then 'cancelled'
            when lower(trim(status)) in ('arrived') then 'arrived'
            else 'other'
        end as status_normalized,

        -- booleans / metadata
        try_to_boolean(iscargo) as is_cargo,
        trim(source) as source,
        try_to_number(schema_version) as schema_version,

        -- timestamps
        window_start_ts_utc,
        window_end_ts_utc,
        sched_dep_ts_utc,
        revised_dep_ts_utc,
        runway_ts_utc,

        -- choose “best” departure time for delay calcs
        coalesce(revised_dep_ts_utc, sched_dep_ts_utc) as dep_best_ts_utc,

        -- derived metric
        datediff('minute', sched_dep_ts_utc, coalesce(revised_dep_ts_utc, sched_dep_ts_utc)) as dep_delay_min,

        -- lineage
        loaded_at,
        source_file,
        row_hash

    from src
)

select
    *,
    -- Your key, built from cleaned fields
    concat_ws(
        '|',
        departure_airport_iata,
        flight_number_clean,
        to_char(sched_dep_ts_utc, 'YYYYMMDDHH24MI')
    ) as flight_key

from clean
where sched_dep_ts_utc is not null;