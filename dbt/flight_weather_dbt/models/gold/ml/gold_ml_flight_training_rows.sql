with snaps as (
  select
    flight_key,
    departure_airport_iata,
    arrival_airport_iata,
    airline_iata,
    flight_number_clean,
    status_normalized,
    sched_dep_ts_utc,
    dep_best_ts_utc,
    dep_delay_min,
    loaded_at_ts_utc
  from {{ ref('stg_fids_flight_snapshots') }}
),

cutoff_base as (
  select
    s.*,
    dateadd('minute', -180, s.sched_dep_ts_utc) as cutoff_ts_utc
  from snaps s
),

flight_cutoff as (
  select *
  from cutoff_base
  where loaded_at_ts_utc <= cutoff_ts_utc
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
    pressure as pressure_hpa,
    humidity as humidity_pct,
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
    on w.airport_iata = f.departure_airport_iata
   and w.obs_ts_utc <= f.cutoff_ts_utc
  qualify row_number() over (
    partition by f.flight_key
    order by w.obs_ts_utc desc
  ) = 1
),

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
  f.departure_airport_iata,
  f.arrival_airport_iata,
  f.airline_iata,
  f.flight_number_clean,
  f.status_normalized as status_at_cutoff,
  f.sched_dep_ts_utc,
  f.cutoff_ts_utc,
  f.loaded_at_ts_utc as snapshot_loaded_at_ts_utc,
  f.dep_delay_min as delay_so_far_min,
  date_part('hour', f.sched_dep_ts_utc) as sched_dep_hour_utc,
  date_part('dow',  f.sched_dep_ts_utc) as sched_dep_dow_utc,
  x.wx_obs_ts_utc,
  x.temp_c,
  x.pressure_hpa,
  x.humidity_pct,
  x.wind_speed_mps,
  x.wind_deg,
  x.visibility_m,
  x.condition_main,
  l.is_delayed_15
from flight_cutoff f
left join wx_asof x on x.flight_key = f.flight_key
left join label_window l on l.flight_key = f.flight_key