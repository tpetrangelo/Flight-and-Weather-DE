USE SCHEMA BRONZE;

CREATE TABLE IF NOT EXISTS BRONZE.FLIGHTS (
    number                          STRING,
    callSign	                    STRING,
    status	                        STRING,
    codeshareStatus	                STRING,
    isCargo	                        BOOLEAN,
    departure_airport_iata	        STRING,
    window_start_utc	            TIMESTAMP_NTZ,
    window_end_utc	                TIMESTAMP_NTZ,
    source	                        STRING,
    schema_version	                INT,
    sched_dep_date	                DATE,
    movement__airport__icao	        STRING,
    movement__airport__iata	        STRING,
    movement__airport__name	        STRING,
    movement__airport__timeZone	    STRING,
    movement__scheduledTime__utc	TIMESTAMP_NTZ,
    movement__scheduledTime__local	TIMESTAMP_TZ,
    movement__revisedTime__utc	    TIMESTAMP_NTZ,
    movement__revisedTime__local	TIMESTAMP_TZ,
    movement__runwayTime__utc	    TIMESTAMP_NTZ,
    movement__runwayTime__local	    TIMESTAMP_TZ,
    movement__terminal	            STRING,
    movement__quality	            VARIANT,
    aircraft__reg	                STRING,
    aircraft__modeS	                STRING,
    aircraft__model	                STRING,
    airline__name	                STRING,
    airline__iata	                STRING,
    airline__icao	                STRING,
    movement__runway                STRING,
    source_file                     STRING,
    loaded_at                       TIMESTAMP_NTZ   DEFAULT (CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ) NOT NULL,
    row_hash                        BINARY(32)
);

CREATE TABLE IF NOT EXISTS BRONZE.WEATHER (
    airport_code            STRING,	
    lat                     FLOAT,	
    lon                     FLOAT,	
    obs_ts_utc              TIMESTAMP_NTZ,	
    temp_c                  NUMBER(6,2),
    feels_like_c	        NUMBER(6,2),
    pressure                NUMBER(4,0),	
    humidity                NUMBER(3,0),	
    wind_speed              NUMBER(4,2),
    wind_deg	            NUMBER(3,0),
    visibility	            NUMBER(5,0),
    weather_main	        STRING,
    weather_desc	        STRING,
    openweather_city_id     INT NOT NULL,
    source_file             STRING,
    loaded_at               TIMESTAMP_NTZ   DEFAULT (CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ) NOT NULL,
    row_hash                BINARY(32)

);

-- Verify tables exist
SHOW TABLES IN SCHEMA FLIGHT_WEATHER.BRONZE;