{{ config(materialized='table', schema='GOLD') }}

with snaps as (
    select
        flight_key,
        dep_airport_iata,
        arr_airport_iata,
        airline_iata,
        flight_number_normalized,
        status_normalized,
        sched_dep_ts_utc,
        dep_best_ts_utc,
        dep_delay_min,
        loaded_at_ts_utc
    from {{ ref('stg_fids_flight_snapshots') }}
),

-- choose ONE snapshot per flight at or before (sched_dep - 60 minutes)
cutoff_candidates as (
    select
        *,
        dateadd('minute', -60, sched_dep_ts_utc) as cutoff_ts_utc,
        row_number() over (
          partition by flight_key
          order by loaded_at_ts_utc desc
        ) as rn_latest_overall
    from snaps
    where loaded_at_ts_utc <= dateadd('minute', -60, sched_dep_ts_utc)
),

flight_cutoff as (
    select *
    from cutoff_candidates
    qualify row_number() over (
      partition by flight_key
      order by loaded_at_ts_utc desc
    ) = 1
),

wx as (
    select
        airport_iata,
        obs_ts_utc,
        temp_c,
        pressure_hpa,
        humidity_pct,
        wind_speed_mps,
        wind_deg,
        visibility_m,
        condition_main
    from {{ ref('stg_weather_observations') }}
),

wx_asof as (
    select
        f.flight_key,
        w.obs_ts_utc as wx_obs_ts_utc,
        w.temp_c,
        w.pressure_hpa,
        w.humidity_pct,
        w.wind_speed_mps,
        w.wind_deg,
        w.visibility_m,
        w.condition_main
    from flight_cutoff f
    join wx w
      on w.airport_iata = f.dep_airport_iata
     and w.obs_ts_utc <= f.cutoff_ts_utc
    qualify row_number() over (
      partition by f.flight_key
      order by w.obs_ts_utc desc
    ) = 1
),

-- labels: for MVP, use "did it ever show >= 15 min delay by departure time window?"
-- this is imperfect but works to get you training. you can refine later.
label_window as (
    select
      s.flight_key,
      max(case when s.dep_delay_min >= 15 then 1 else 0 end) as is_delayed_15
    from snaps s
    where s.loaded_at_ts_utc <= dateadd('hour', 6, s.sched_dep_ts_utc)
    group by 1
)

select
    f.flight_key,
    f.dep_airport_iata,
    f.arr_airport_iata,
    f.airline_iata,
    f.flight_number_normalized,
    f.status_normalized as status_at_cutoff,
    f.sched_dep_ts_utc,
    f.cutoff_ts_utc,
    f.loaded_at_ts_utc as snapshot_loaded_at_ts_utc,
    f.dep_delay_min as delay_so_far_min,

    -- time features
    date_part('hour', f.sched_dep_ts_utc) as sched_dep_hour_utc,
    date_part('dow',  f.sched_dep_ts_utc) as sched_dep_dow_utc,

    -- weather as-of features
    x.wx_obs_ts_utc,
    x.temp_c,
    x.pressure_hpa,
    x.humidity_pct,
    x.wind_speed_mps,
    x.wind_deg,
    x.visibility_m,
    x.condition_main,

    -- label
    l.is_delayed_15
from flight_cutoff f
left join wx_asof x
  on x.flight_key = f.flight_key
left join label_window l
  on l.flight_key = f.flight_key
;