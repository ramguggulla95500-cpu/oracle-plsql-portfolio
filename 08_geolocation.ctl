-- FILE: sqlldr/08_geolocation.ctl
-- SOURCE: olist_geolocation_dataset.csv
-- NOTE: ~1 Million rows — use DIRECT=TRUE for performance

LOAD DATA
INFILE '/data/olist/csv/olist_geolocation_dataset.csv'
BADFILE '/data/olist/logs/geolocation.bad'
APPEND
INTO TABLE STG.STG_GEOLOCATION
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
    GEOLOCATION_ZIP_CODE_PREFIX CHAR(10),
    GEOLOCATION_LAT             CHAR(30),
    GEOLOCATION_LNG             CHAR(30),
    GEOLOCATION_CITY            CHAR(100),
    GEOLOCATION_STATE           CHAR(5),
    ETL_BATCH_ID                CONSTANT "0",
    ETL_INSERT_DT               SYSDATE,
    ETL_SOURCE_FILE             CONSTANT "olist_geolocation_dataset.csv"
)
