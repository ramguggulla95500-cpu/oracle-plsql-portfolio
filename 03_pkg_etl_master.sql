-- =============================================================================
-- PROJECT   : Olist Brazilian E-Commerce ETL Data Pipeline
-- FILE      : packages/03_pkg_etl_master.sql
-- PURPOSE   : Master orchestrator — coordinates SQL*Loader + Transform + Scheduler
-- SCHEMA    : ETL_CTRL
-- =============================================================================

CREATE OR REPLACE PACKAGE ETL_CTRL.PKG_ETL_MASTER AS

    -- Full pipeline (call after SQL*Loader loads staging)
    PROCEDURE run_pipeline(p_triggered_by IN VARCHAR2 DEFAULT 'SCHEDULER');

    -- Update batch_id on staging tables after SQL*Loader load
    PROCEDURE tag_staging_batch(p_batch_id IN NUMBER);

    -- Setup all DBMS_SCHEDULER jobs
    PROCEDURE setup_scheduler_jobs;

    -- Manually trigger a re-run for a specific batch
    PROCEDURE rerun_batch(p_batch_id IN NUMBER);

    -- Health check — returns status of last 5 batches
    PROCEDURE health_check;

END PKG_ETL_MASTER;
/

CREATE OR REPLACE PACKAGE BODY ETL_CTRL.PKG_ETL_MASTER AS

    -- ================================================================
    -- Tag staging tables with current batch_id (after SQL*Loader)
    -- SQL*Loader sets BATCH_ID = 0; this updates to the real batch_id
    -- ================================================================
    PROCEDURE tag_staging_batch(p_batch_id IN NUMBER) IS
        v_step_id NUMBER;
    BEGIN
        v_step_id := ETL_CTRL.PKG_ETL_LOGGER.start_step(p_batch_id, 'TAG_STAGING_BATCH');

        UPDATE STG.STG_ORDERS           SET ETL_BATCH_ID = p_batch_id WHERE ETL_BATCH_ID = 0;
        UPDATE STG.STG_ORDER_ITEMS      SET ETL_BATCH_ID = p_batch_id WHERE ETL_BATCH_ID = 0;
        UPDATE STG.STG_ORDER_PAYMENTS   SET ETL_BATCH_ID = p_batch_id WHERE ETL_BATCH_ID = 0;
        UPDATE STG.STG_ORDER_REVIEWS    SET ETL_BATCH_ID = p_batch_id WHERE ETL_BATCH_ID = 0;
        UPDATE STG.STG_CUSTOMERS        SET ETL_BATCH_ID = p_batch_id WHERE ETL_BATCH_ID = 0;
        UPDATE STG.STG_SELLERS          SET ETL_BATCH_ID = p_batch_id WHERE ETL_BATCH_ID = 0;
        UPDATE STG.STG_PRODUCTS         SET ETL_BATCH_ID = p_batch_id WHERE ETL_BATCH_ID = 0;
        UPDATE STG.STG_GEOLOCATION      SET ETL_BATCH_ID = p_batch_id WHERE ETL_BATCH_ID = 0;
        UPDATE STG.STG_CATEGORY_TRANSLATION SET ETL_BATCH_ID = p_batch_id WHERE ETL_BATCH_ID = 0;
        COMMIT;

        ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id, 'SUCCESS');
    EXCEPTION
        WHEN OTHERS THEN
            ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id, 'FAILED', p_err => SQLERRM);
            RAISE;
    END tag_staging_batch;

    -- ================================================================
    -- MAIN PIPELINE — called by scheduler or manually
    -- ================================================================
    PROCEDURE run_pipeline(p_triggered_by IN VARCHAR2 DEFAULT 'SCHEDULER') IS
        v_batch_id  NUMBER;
        v_start     DATE := SYSDATE;
    BEGIN
        -- 1. Open batch
        v_batch_id := ETL_CTRL.PKG_ETL_LOGGER.start_batch(
            p_batch_name => 'OLIST_ETL_' || TO_CHAR(SYSDATE, 'YYYYMMDD_HH24MISS'),
            p_pipeline   => 'OLIST_ECOMMERCE_PIPELINE',
            p_triggered_by => p_triggered_by
        );

        DBMS_OUTPUT.PUT_LINE('=========================================');
        DBMS_OUTPUT.PUT_LINE('OLIST ETL Pipeline Started');
        DBMS_OUTPUT.PUT_LINE('Batch ID : ' || v_batch_id);
        DBMS_OUTPUT.PUT_LINE('Started  : ' || TO_CHAR(v_start,'YYYY-MM-DD HH24:MI:SS'));
        DBMS_OUTPUT.PUT_LINE('=========================================');

        -- 2. Tag staging rows with this batch_id
        --    (SQL*Loader already populated staging with ETL_BATCH_ID=0)
        tag_staging_batch(v_batch_id);

        -- 3. Run all transforms
        ETL_CTRL.PKG_TRANSFORM.run_all(v_batch_id);

        -- 4. Close batch
        ETL_CTRL.PKG_ETL_LOGGER.end_batch(v_batch_id, 'SUCCESS');

        DBMS_OUTPUT.PUT_LINE('Pipeline SUCCESS | Duration: '
            || ROUND((SYSDATE - v_start)*60,1) || ' mins | Batch: ' || v_batch_id);

    EXCEPTION
        WHEN OTHERS THEN
            ETL_CTRL.PKG_ETL_LOGGER.end_batch(v_batch_id, 'FAILED', SQLERRM);
            DBMS_OUTPUT.PUT_LINE('PIPELINE FAILED | Batch: ' || v_batch_id);
            DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
            RAISE;
    END run_pipeline;

    -- ================================================================
    -- RE-RUN a previously failed batch
    -- ================================================================
    PROCEDURE rerun_batch(p_batch_id IN NUMBER) IS
    BEGIN
        -- Reset batch status to allow re-run
        UPDATE ETL_CTRL.ETL_BATCH_LOG
        SET STATUS = 'RERUN', ERROR_MESSAGE = NULL
        WHERE BATCH_ID = p_batch_id;
        COMMIT;

        -- Re-run transforms (staging data still present)
        ETL_CTRL.PKG_TRANSFORM.run_all(p_batch_id);
        ETL_CTRL.PKG_ETL_LOGGER.end_batch(p_batch_id, 'SUCCESS');
        DBMS_OUTPUT.PUT_LINE('Rerun complete for batch: ' || p_batch_id);
    EXCEPTION
        WHEN OTHERS THEN
            ETL_CTRL.PKG_ETL_LOGGER.end_batch(p_batch_id, 'FAILED', SQLERRM);
            RAISE;
    END rerun_batch;

    -- ================================================================
    -- HEALTH CHECK — quick status view
    -- ================================================================
    PROCEDURE health_check IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('=== ETL HEALTH CHECK ===');
        FOR r IN (
            SELECT BATCH_ID, BATCH_NAME, STATUS,
                   TO_CHAR(START_DT,'YYYY-MM-DD HH24:MI') AS STARTED,
                   TOTAL_RECORDS, REJECTED_RECORDS,
                   ROUND((END_DT - START_DT)*60,1) AS DURATION_MINS
            FROM ETL_CTRL.ETL_BATCH_LOG
            ORDER BY START_DT DESC
            FETCH FIRST 5 ROWS ONLY
        ) LOOP
            DBMS_OUTPUT.PUT_LINE(
                'Batch: ' || r.BATCH_ID ||
                ' | ' || r.STATUS ||
                ' | Started: ' || r.STARTED ||
                ' | Records: ' || r.TOTAL_RECORDS ||
                ' | Rejected: ' || r.REJECTED_RECORDS ||
                ' | Duration: ' || NVL(TO_CHAR(r.DURATION_MINS),'RUNNING') || ' mins'
            );
        END LOOP;
    END health_check;

    -- ================================================================
    -- SETUP DBMS_SCHEDULER JOBS
    -- ================================================================
    PROCEDURE setup_scheduler_jobs IS

        PROCEDURE drop_if_exists(p_job IN VARCHAR2) IS
        BEGIN
            DBMS_SCHEDULER.DROP_JOB('ETL_CTRL.' || p_job, FORCE => TRUE);
        EXCEPTION WHEN OTHERS THEN NULL;
        END;

    BEGIN
        -- ------------------------------------------------
        -- JOB 1: Daily Full Pipeline — runs at 2:00 AM
        -- Assumes SQL*Loader shell script has already run
        -- ------------------------------------------------
        drop_if_exists('JOB_OLIST_DAILY_ETL');
        DBMS_SCHEDULER.CREATE_JOB(
            job_name        => 'ETL_CTRL.JOB_OLIST_DAILY_ETL',
            job_type        => 'PLSQL_BLOCK',
            job_action      => 'BEGIN ETL_CTRL.PKG_ETL_MASTER.run_pipeline(''SCHEDULER''); END;',
            start_date      => TRUNC(SYSDATE+1) + 2/24,    -- Next day 02:00 AM
            repeat_interval => 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0;BYSECOND=0',
            enabled         => TRUE,
            auto_drop       => FALSE,
            comments        => 'Daily Olist ETL Full Pipeline at 2AM'
        );

        -- ------------------------------------------------
        -- JOB 2: Hourly Aggregate Refresh
        -- ------------------------------------------------
        drop_if_exists('JOB_OLIST_HOURLY_AGG');
        DBMS_SCHEDULER.CREATE_JOB(
            job_name        => 'ETL_CTRL.JOB_OLIST_HOURLY_AGG',
            job_type        => 'PLSQL_BLOCK',
            job_action      => '
                DECLARE v_b NUMBER;
                BEGIN
                    v_b := ETL_CTRL.PKG_ETL_LOGGER.start_batch(
                        ''HOURLY_AGG_''||TO_CHAR(SYSDATE,''HH24MISS''),
                        ''HOURLY_AGG'', ''SCHEDULER'');
                    ETL_CTRL.PKG_TRANSFORM.refresh_agg_daily_sales(v_b);
                    ETL_CTRL.PKG_ETL_LOGGER.end_batch(v_b,''SUCCESS'');
                END;',
            start_date      => TRUNC(SYSDATE) + 1/24,
            repeat_interval => 'FREQ=HOURLY;INTERVAL=1',
            enabled         => TRUE,
            auto_drop       => FALSE,
            comments        => 'Hourly refresh of AGG_DAILY_SALES for BI'
        );

        -- ------------------------------------------------
        -- JOB 3: Health Check — every 30 minutes to alert log
        -- ------------------------------------------------
        drop_if_exists('JOB_OLIST_HEALTH_CHECK');
        DBMS_SCHEDULER.CREATE_JOB(
            job_name        => 'ETL_CTRL.JOB_OLIST_HEALTH_CHECK',
            job_type        => 'PLSQL_BLOCK',
            job_action      => 'BEGIN ETL_CTRL.PKG_ETL_MASTER.health_check; END;',
            start_date      => SYSDATE,
            repeat_interval => 'FREQ=MINUTELY;INTERVAL=30',
            enabled         => TRUE,
            auto_drop       => FALSE,
            comments        => 'ETL pipeline health check every 30 minutes'
        );

        DBMS_OUTPUT.PUT_LINE('All scheduler jobs created:');
        DBMS_OUTPUT.PUT_LINE('  JOB_OLIST_DAILY_ETL    -> Daily at 2:00 AM');
        DBMS_OUTPUT.PUT_LINE('  JOB_OLIST_HOURLY_AGG   -> Every 1 hour');
        DBMS_OUTPUT.PUT_LINE('  JOB_OLIST_HEALTH_CHECK -> Every 30 minutes');

    END setup_scheduler_jobs;

END PKG_ETL_MASTER;
/
