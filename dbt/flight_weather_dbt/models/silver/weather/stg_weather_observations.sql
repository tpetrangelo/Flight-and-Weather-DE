with src as (
    select
        *
    from bronze.weather
),

clean as (
    select
        -- identifiers
        upper(trim(airport_code)) as airport_iata,

        -- geo (keep explicit so you can join to reference tables later)
        lat as airport_latitude,
        lon as airport_longitude,

        -- time
        obs_ts_utc,
        loaded_at as loaded_at_ts_utc,

        -- measurements (units explicit)
        temp_c,
        feels_like_c,
        pressure,
        humidity,

        wind_speed as wind_speed_mps,
        wind_deg,

        visibility as visibility_m,

        -- conditions
        weather_main as condition_main,
        weather_desc as condition_description,

        -- lineage
        openweather_city_id,
        source_file,
        row_hash

    from src
    where obs_ts_utc is not null
      and airport_code is not null
),

dedup as (
    select
        *,
        row_number() over (
            partition by airport_iata, obs_ts_utc
            order by
                loaded_at_ts_utc desc,
                row_hash desc,
                source_file desc
        ) as rn
    from clean
)

select
    * exclude (rn)
from dedup
where rn = 1