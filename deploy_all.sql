-- =============================================================================
-- PROJECT   : Olist Brazilian E-Commerce ETL Data Pipeline
-- FILE      : scripts/deploy_all.sql
-- PURPOSE   : Deploy complete project in one shot
-- RUN AS    : First run 01_create_schemas.sql as SYSDBA, then run this as ETL_CTRL
-- CONNECT   : sqlplus ETL_CTRL/EtlPass123#@//localhost:1521/ORCL @scripts/deploy_all.sql
-- =============================================================================

PROMPT ======================================================
PROMPT   Olist ETL Pipeline — Full Deployment
PROMPT ======================================================

PROMPT
PROMPT [1/6] Creating Staging Tables...
@sql/02_staging_ddl.sql

PROMPT
PROMPT [2/6] Creating DWH Tables + Populating DIM_DATE...
@sql/03_dwh_ddl.sql

PROMPT
PROMPT [3/6] Creating ETL Logger Package...
@packages/01_pkg_etl_logger.sql

PROMPT
PROMPT [4/6] Creating Transform Package...
@packages/02_pkg_transform.sql

PROMPT
PROMPT [5/6] Creating Master Orchestrator Package...
@packages/03_pkg_etl_master.sql

PROMPT
PROMPT [6/6] Setting up DBMS_SCHEDULER Jobs...
BEGIN
    ETL_CTRL.PKG_ETL_MASTER.setup_scheduler_jobs;
END;
/

PROMPT
PROMPT ======================================================
PROMPT   Deployment Complete!
PROMPT
PROMPT   NEXT STEPS:
PROMPT   1. Download Olist CSVs from Kaggle to /data/olist/csv/
PROMPT   2. Run: chmod +x scripts/run_sqlldr.sh
PROMPT   3. Run: ./scripts/run_sqlldr.sh
PROMPT   OR manually call:
PROMPT      BEGIN ETL_CTRL.PKG_ETL_MASTER.run_pipeline; END;
PROMPT ======================================================

SELECT 'Deployed at: ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') STATUS FROM DUAL;
