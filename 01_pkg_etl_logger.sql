-- =============================================================================
-- PROJECT   : Olist Brazilian E-Commerce ETL Data Pipeline
-- FILE      : packages/01_pkg_etl_logger.sql
-- PURPOSE   : Centralized batch logging, watermark management, rejection handler
-- SCHEMA    : ETL_CTRL
-- =============================================================================

CREATE OR REPLACE PACKAGE ETL_CTRL.PKG_ETL_LOGGER AS

    FUNCTION  start_batch(p_batch_name IN VARCHAR2, p_pipeline IN VARCHAR2,
                          p_triggered_by IN VARCHAR2 DEFAULT 'SCHEDULER') RETURN NUMBER;

    FUNCTION  start_step(p_batch_id IN NUMBER, p_step_name IN VARCHAR2) RETURN NUMBER;

    PROCEDURE end_step(p_step_id IN NUMBER, p_status IN VARCHAR2,
                       p_in IN NUMBER DEFAULT 0, p_out IN NUMBER DEFAULT 0,
                       p_rej IN NUMBER DEFAULT 0, p_err IN VARCHAR2 DEFAULT NULL);

    PROCEDURE end_batch(p_batch_id IN NUMBER, p_status IN VARCHAR2,
                        p_err IN VARCHAR2 DEFAULT NULL);

    FUNCTION  get_watermark(p_source IN VARCHAR2, p_entity IN VARCHAR2) RETURN DATE;

    PROCEDURE set_watermark(p_source IN VARCHAR2, p_entity IN VARCHAR2,
                            p_dt IN DATE, p_batch_id IN NUMBER);

    PROCEDURE log_reject(p_batch_id IN NUMBER, p_table IN VARCHAR2,
                         p_key IN VARCHAR2, p_reason IN VARCHAR2,
                         p_raw IN CLOB DEFAULT NULL);

END PKG_ETL_LOGGER;
/

CREATE OR REPLACE PACKAGE BODY ETL_CTRL.PKG_ETL_LOGGER AS

    FUNCTION start_batch(p_batch_name IN VARCHAR2, p_pipeline IN VARCHAR2,
                         p_triggered_by IN VARCHAR2 DEFAULT 'SCHEDULER') RETURN NUMBER IS
        v_id NUMBER;
    BEGIN
        INSERT INTO ETL_CTRL.ETL_BATCH_LOG
            (BATCH_NAME, PIPELINE_NAME, START_DT, STATUS, TRIGGERED_BY)
        VALUES (p_batch_name, p_pipeline, SYSDATE, 'RUNNING', p_triggered_by)
        RETURNING BATCH_ID INTO v_id;
        COMMIT;
        RETURN v_id;
    END start_batch;

    FUNCTION start_step(p_batch_id IN NUMBER, p_step_name IN VARCHAR2) RETURN NUMBER IS
        v_id NUMBER;
    BEGIN
        INSERT INTO ETL_CTRL.ETL_STEP_LOG (BATCH_ID, STEP_NAME, STEP_STATUS, START_DT)
        VALUES (p_batch_id, p_step_name, 'RUNNING', SYSDATE)
        RETURNING STEP_LOG_ID INTO v_id;
        COMMIT;
        RETURN v_id;
    END start_step;

    PROCEDURE end_step(p_step_id IN NUMBER, p_status IN VARCHAR2,
                       p_in IN NUMBER DEFAULT 0, p_out IN NUMBER DEFAULT 0,
                       p_rej IN NUMBER DEFAULT 0, p_err IN VARCHAR2 DEFAULT NULL) IS
    BEGIN
        UPDATE ETL_CTRL.ETL_STEP_LOG
        SET STEP_STATUS      = p_status,
            END_DT           = SYSDATE,
            RECORDS_IN       = p_in,
            RECORDS_OUT      = p_out,
            RECORDS_REJECTED = p_rej,
            ERROR_MESSAGE    = SUBSTR(p_err, 1, 4000)
        WHERE STEP_LOG_ID = p_step_id;
        COMMIT;
    END end_step;

    PROCEDURE end_batch(p_batch_id IN NUMBER, p_status IN VARCHAR2,
                        p_err IN VARCHAR2 DEFAULT NULL) IS
        v_in  NUMBER; v_out NUMBER; v_rej NUMBER;
    BEGIN
        SELECT NVL(SUM(RECORDS_IN),0), NVL(SUM(RECORDS_OUT),0), NVL(SUM(RECORDS_REJECTED),0)
        INTO v_in, v_out, v_rej
        FROM ETL_CTRL.ETL_STEP_LOG WHERE BATCH_ID = p_batch_id;

        UPDATE ETL_CTRL.ETL_BATCH_LOG
        SET STATUS           = p_status,
            END_DT           = SYSDATE,
            TOTAL_RECORDS    = v_in,
            SUCCESS_RECORDS  = v_out,
            REJECTED_RECORDS = v_rej,
            ERROR_MESSAGE    = SUBSTR(p_err, 1, 4000)
        WHERE BATCH_ID = p_batch_id;
        COMMIT;
    END end_batch;

    FUNCTION get_watermark(p_source IN VARCHAR2, p_entity IN VARCHAR2) RETURN DATE IS
        v_dt DATE;
    BEGIN
        SELECT LAST_LOAD_DT INTO v_dt
        FROM ETL_CTRL.ETL_WATERMARK
        WHERE SOURCE_NAME = p_source AND ENTITY_NAME = p_entity;
        RETURN NVL(v_dt, DATE '2015-01-01');
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN DATE '2015-01-01';
    END get_watermark;

    PROCEDURE set_watermark(p_source IN VARCHAR2, p_entity IN VARCHAR2,
                            p_dt IN DATE, p_batch_id IN NUMBER) IS
    BEGIN
        MERGE INTO ETL_CTRL.ETL_WATERMARK tgt
        USING DUAL ON (tgt.SOURCE_NAME = p_source AND tgt.ENTITY_NAME = p_entity)
        WHEN MATCHED THEN UPDATE SET
            LAST_LOAD_DT = p_dt, LAST_BATCH_ID = p_batch_id, ETL_UPDATE_DT = SYSDATE
        WHEN NOT MATCHED THEN INSERT
            (SOURCE_NAME, ENTITY_NAME, LAST_LOAD_DT, LAST_BATCH_ID, ETL_UPDATE_DT)
        VALUES (p_source, p_entity, p_dt, p_batch_id, SYSDATE);
        COMMIT;
    END set_watermark;

    PROCEDURE log_reject(p_batch_id IN NUMBER, p_table IN VARCHAR2,
                         p_key IN VARCHAR2, p_reason IN VARCHAR2,
                         p_raw IN CLOB DEFAULT NULL) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO STG.STG_REJECTED_RECORDS
            (SOURCE_TABLE, SOURCE_KEY, REJECT_REASON, RAW_DATA, ETL_BATCH_ID)
        VALUES (p_table, p_key, SUBSTR(p_reason,1,1000), p_raw, p_batch_id);
        COMMIT;
    END log_reject;

END PKG_ETL_LOGGER;
/
