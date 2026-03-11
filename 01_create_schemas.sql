-- =============================================================================
-- PROJECT   : Olist Brazilian E-Commerce ETL Data Pipeline
-- FILE      : 01_create_schemas.sql
-- PURPOSE   : Create Oracle Schemas and grant privileges
-- RUN AS    : SYSDBA / DBA
-- DATABASE  : Oracle 19c+
-- AUTHOR    : Data Engineering Team
-- VERSION   : 1.0 | February 2026
-- =============================================================================
-- SOURCE DATASET : Kaggle - Brazilian E-Commerce Public Dataset by Olist
-- DOWNLOAD  : https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
-- =============================================================================

-- Step 1: Create Schemas (Users)
CREATE USER STG      IDENTIFIED BY StgPass123#
    DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;

CREATE USER DWH      IDENTIFIED BY DwhPass123#
    DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;

CREATE USER ETL_CTRL IDENTIFIED BY EtlPass123#
    DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;

-- Step 2: Grant Privileges
GRANT CREATE SESSION, CREATE TABLE, CREATE INDEX,
      CREATE SEQUENCE, CREATE VIEW TO STG, DWH, ETL_CTRL;

GRANT CREATE PROCEDURE, CREATE JOB, CREATE TYPE TO ETL_CTRL;

-- Allow ETL_CTRL to read/write STG and DWH tables
GRANT SELECT, INSERT, UPDATE, DELETE ON STG TO ETL_CTRL;
GRANT SELECT, INSERT, UPDATE, DELETE ON DWH TO ETL_CTRL;

-- Allow ETL_CTRL to create objects in STG and DWH schemas
GRANT CREATE ANY TABLE TO ETL_CTRL;

-- Oracle Directory for SQL*Loader flat files
-- Update path to your actual CSV download location
CREATE OR REPLACE DIRECTORY OLIST_DATA_DIR AS '/data/olist/csv/';
GRANT READ, WRITE ON DIRECTORY OLIST_DATA_DIR TO STG, ETL_CTRL;

COMMIT;
