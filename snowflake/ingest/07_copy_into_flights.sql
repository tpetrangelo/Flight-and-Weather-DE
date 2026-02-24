-- Make the task self-contained and predictable in Airflow
USE ROLE ROLE_FLIGHT_WEATHER_ETL;
USE WAREHOUSE FLIGHT_WEATHER_INGEST_WH;
USE DATABASE FLIGHT_WEATHER;
USE SCHEMA BRONZE;

COPY INTO BRONZE.FLIGHTS (
    number,
    callSign,
    status,
    codeshareStatus,
    isCargo,
    departure_airport_iata,
    window_start_utc,
    window_end_utc,
    source,
    schema_version,
    sched_dep_date,
    movement__airport__icao,
    movement__airport__iata,
    movement__airport__name,
    movement__airport__timeZone,
    movement__scheduledTime__utc,
    movement__scheduledTime__local,
    movement__revisedTime__utc,
    movement__revisedTime__local,
    movement__runwayTime__utc,
    movement__runwayTime__local,
    movement__terminal,
    movement__quality,
    aircraft__reg,
    aircraft__modeS,
    aircraft__model,
    airline__name,
    airline__iata,
    airline__icao,
    movement__runway,
    source_file,
    loaded_at,
    row_hash
)
FROM (
  SELECT
    $1:number::STRING                                  AS number,
    $1:callSign::STRING                                AS callSign,
    $1:status::STRING                                  AS status,
    $1:codeshareStatus::STRING                         AS codeshareStatus,
    $1:isCargo::BOOLEAN                                AS isCargo,
    $1:departure_airport_iata::STRING                  AS departure_airport_iata,

    TRY_TO_TIMESTAMP_NTZ($1:window_start_utc::STRING)  AS window_start_utc,
    TRY_TO_TIMESTAMP_NTZ($1:window_end_utc::STRING)    AS window_end_utc,

    $1:source::STRING                                  AS source,
    $1:schema_version::INT                             AS schema_version,
    TRY_TO_DATE($1:sched_dep_date::STRING)             AS sched_dep_date,

    $1:movement__airport__icao::STRING                 AS movement__airport__icao,
    $1:movement__airport__iata::STRING                 AS movement__airport__iata,
    $1:movement__airport__name::STRING                 AS movement__airport__name,
    $1:movement__airport__timeZone::STRING             AS movement__airport__timeZone,

    TRY_TO_TIMESTAMP_NTZ($1:movement__scheduledTime__utc::STRING)   AS movement__scheduledTime__utc,
    TRY_TO_TIMESTAMP_TZ($1:movement__scheduledTime__local::STRING) AS movement__scheduledTime__local,
    TRY_TO_TIMESTAMP_NTZ($1:movement__revisedTime__utc::STRING)     AS movement__revisedTime__utc,
    TRY_TO_TIMESTAMP_TZ($1:movement__revisedTime__local::STRING)   AS movement__revisedTime__local,
    TRY_TO_TIMESTAMP_NTZ($1:movement__runwayTime__utc::STRING)      AS movement__runwayTime__utc,
    TRY_TO_TIMESTAMP_TZ($1:movement__runwayTime__local::STRING)    AS movement__runwayTime__local,

    $1:movement__terminal::STRING                      AS movement__terminal,
    $1:movement__quality                               AS movement__quality,  -- keep as VARIANT-compatible column type

    $1:aircraft__reg::STRING                           AS aircraft__reg,
    $1:aircraft__modeS::STRING                         AS aircraft__modeS,
    $1:aircraft__model::STRING                         AS aircraft__model,

    $1:airline__name::STRING                           AS airline__name,
    $1:airline__iata::STRING                           AS airline__iata,
    $1:airline__icao::STRING                           AS airline__icao,

    $1:movement__runway::STRING                        AS movement__runway,

    METADATA$FILENAME                                  AS source_file,
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())       AS loaded_at,

    -- Stable hash: hash of the core business identity fields (plus scheduled time + airports).
    -- This avoids hashing the entire VARIANT, which can be sensitive to serialization differences.
    SHA2(
      CONCAT_WS('|',
        COALESCE($1:number::STRING, ''),
        COALESCE($1:callSign::STRING, ''),
        COALESCE($1:departure_airport_iata::STRING, ''),
        COALESCE($1:movement__airport__iata::STRING, ''),
        COALESCE(TO_VARCHAR(TRY_TO_TIMESTAMP_NTZ($1:movement__scheduledTime__utc::STRING)), ''),
        COALESCE(TO_VARCHAR(TRY_TO_TIMESTAMP_NTZ($1:window_start_utc::STRING)), ''),
        COALESCE(TO_VARCHAR(TRY_TO_TIMESTAMP_NTZ($1:window_end_utc::STRING)), '')
      ),
      256
    ) AS row_hash

  FROM @INGEST.STG_FLIGHTS_BRONZE
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';
