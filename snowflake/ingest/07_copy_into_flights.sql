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
    loaded_at
)
FROM (
  SELECT
    $1:number::STRING                              AS number,
    $1:callSign::STRING                            AS callSign,
    $1:status::STRING                              AS status,
    $1:codeshareStatus::STRING                     AS codeshareStatus,
    $1:isCargo::BOOLEAN                            AS isCargo,
    $1:departure_airport_iata::STRING              AS departure_airport_iata,
    TRY_TO_TIMESTAMP_NTZ($1:window_start_utc::STRING) AS window_start_utc,
    $1:window_end_utc::TIMESTAMP_NTZ               AS window_end_utc,
    $1:source::STRING                              AS source,
    $1:schema_version::INT                         AS schema_version,
    TRY_TO_DATE($1:sched_dep_date::STRING)            AS sched_dep_date,


    $1:movement__airport__icao::STRING             AS movement__airport__icao,
    $1:movement__airport__iata::STRING             AS movement__airport__iata,
    $1:movement__airport__name::STRING             AS movement__airport__name,
    $1:movement__airport__timeZone::STRING         AS movement__airport__timeZone,

    $1:movement__scheduledTime__utc::TIMESTAMP_NTZ AS movement__scheduledTime__utc,
    $1:movement__scheduledTime__local::TIMESTAMP_NTZ AS movement__scheduledTime__local,
    $1:movement__revisedTime__utc::TIMESTAMP_NTZ   AS movement__revisedTime__utc,
    $1:movement__revisedTime__local::TIMESTAMP_NTZ AS movement__revisedTime__local,
    $1:movement__runwayTime__utc::TIMESTAMP_NTZ    AS movement__runwayTime__utc,
    $1:movement__runwayTime__local::TIMESTAMP_NTZ  AS movement__runwayTime__local,

    $1:movement__terminal::STRING                  AS movement__terminal,
    $1:movement__quality                           AS movement__quality,

    $1:aircraft__reg::STRING                       AS aircraft__reg,
    $1:aircraft__modeS::STRING                     AS aircraft__modeS,
    $1:aircraft__model::STRING                     AS aircraft__model,

    $1:airline__name::STRING                       AS airline__name,
    $1:airline__iata::STRING                       AS airline__iata,
    $1:airline__icao::STRING                       AS airline__icao,

    $1:movement__runway::STRING                    AS movement__runway,

    METADATA$FILENAME                              AS source_file,
    CURRENT_TIMESTAMP()                            AS loaded_at
  FROM @INGEST.STG_FLIGHTS_BRONZE
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';