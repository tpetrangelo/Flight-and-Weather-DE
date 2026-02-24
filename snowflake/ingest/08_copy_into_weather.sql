-- Make the task self-contained and predictable in Airflow
USE ROLE ROLE_FLIGHT_WEATHER_ETL;
USE WAREHOUSE FLIGHT_WEATHER_INGEST_WH;
USE DATABASE FLIGHT_WEATHER;
USE SCHEMA BRONZE;

COPY INTO BRONZE.WEATHER (
    airport_code,
    lat,
    lon,
    obs_ts_utc,
    temp_c,
    feels_like_c,
    pressure,
    humidity,
    wind_speed,
    wind_deg,
    visibility,
    weather_main,
    weather_desc,
    openweather_city_id,
    source_file,
    loaded_at,
    row_hash
)
FROM (
    SELECT
        $1:airport_code::STRING                          AS airport_code,
        $1:lat::FLOAT                                    AS lat,
        $1:lon::FLOAT                                    AS lon,
        TRY_TO_TIMESTAMP_NTZ($1:obs_ts_utc::STRING)      AS obs_ts_utc,

        $1:temp_c::NUMBER(6,2)                           AS temp_c,
        $1:feels_like_c::NUMBER(6,2)                     AS feels_like_c,
        $1:pressure::NUMBER(6,0)                         AS pressure,
        $1:humidity::NUMBER(3,0)                         AS humidity,
        $1:wind_speed::NUMBER(6,2)                       AS wind_speed,
        $1:wind_deg::NUMBER(3,0)                         AS wind_deg,
        $1:visibility::NUMBER(10,0)                      AS visibility,
        $1:weather_main::STRING                          AS weather_main,
        $1:weather_desc::STRING                          AS weather_desc,
        $1:openweather_city_id::INT                      AS openweather_city_id,

        METADATA$FILENAME                                AS source_file,
        convert_timezone('UTC', current_timestamp())     AS loaded_at,

        -- Weather grain hash: airport + observation timestamp (+ coords as tie-breakers)
        SHA2(
          CONCAT_WS('|',
            COALESCE($1:airport_code::STRING, ''),
            COALESCE(TO_VARCHAR(TRY_TO_TIMESTAMP_NTZ($1:obs_ts_utc::STRING)), ''),
            COALESCE(TO_VARCHAR($1:lat::FLOAT), ''),
            COALESCE(TO_VARCHAR($1:lon::FLOAT), '')
          ),
          256
        ) AS row_hash

    FROM @INGEST.STG_WEATHER_BRONZE
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';
