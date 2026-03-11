-- =============================================================================
-- PROJECT   : Olist Brazilian E-Commerce ETL Data Pipeline
-- FILE      : 03_dwh_ddl.sql
-- PURPOSE   : Data Warehouse Star Schema — Dimensions + Fact Tables
-- SCHEMA    : DWH
-- VERSION   : 1.0 | February 2026
-- =============================================================================

-- ============================================================
-- DIMENSION: DIM_DATE
-- Pre-populated calendar table. No ETL load required.
-- ============================================================
CREATE TABLE DWH.DIM_DATE (
    DATE_KEY            NUMBER          NOT NULL,   -- Format: YYYYMMDD
    FULL_DATE           DATE            NOT NULL,
    DAY_NUM             NUMBER(2),
    DAY_NAME            VARCHAR2(10),               -- Monday, Tuesday...
    WEEK_NUM            NUMBER(2),
    MONTH_NUM           NUMBER(2),
    MONTH_NAME          VARCHAR2(15),
    QUARTER_NUM         NUMBER(1),
    QUARTER_NAME        VARCHAR2(6),                -- Q1, Q2...
    YEAR_NUM            NUMBER(4),
    IS_WEEKEND          CHAR(1)         DEFAULT 'N',
    IS_HOLIDAY          CHAR(1)         DEFAULT 'N',
    CONSTRAINT DIM_DATE_PK PRIMARY KEY (DATE_KEY)
);

-- ============================================================
-- DIMENSION: DIM_CUSTOMER (SCD Type 1)
-- Source: STG_CUSTOMERS + STG_GEOLOCATION
-- ============================================================
CREATE TABLE DWH.DIM_CUSTOMER (
    CUSTOMER_KEY            NUMBER          GENERATED ALWAYS AS IDENTITY,
    CUSTOMER_UNIQUE_ID      VARCHAR2(50)    NOT NULL,   -- True business key
    CUSTOMER_ID             VARCHAR2(50),               -- Order-level ID
    ZIP_CODE_PREFIX         VARCHAR2(10),
    CITY                    VARCHAR2(100),
    STATE                   VARCHAR2(5),
    STATE_NAME              VARCHAR2(100),              -- Derived full state name
    REGION                  VARCHAR2(30),               -- Derived: North/South/Southeast etc.
    LAT                     NUMBER(10,6),               -- From geolocation join
    LNG                     NUMBER(10,6),
    IS_ACTIVE               CHAR(1)         DEFAULT 'Y',
    ETL_BATCH_ID            NUMBER,
    ETL_INSERT_DT           DATE            DEFAULT SYSDATE,
    ETL_UPDATE_DT           DATE,
    CONSTRAINT DIM_CUSTOMER_PK PRIMARY KEY (CUSTOMER_KEY),
    CONSTRAINT DIM_CUSTOMER_BK UNIQUE (CUSTOMER_UNIQUE_ID)
);
CREATE INDEX DIM_CUST_STATE_IDX ON DWH.DIM_CUSTOMER (STATE);

-- ============================================================
-- DIMENSION: DIM_SELLER (SCD Type 1)
-- Source: STG_SELLERS + STG_GEOLOCATION
-- ============================================================
CREATE TABLE DWH.DIM_SELLER (
    SELLER_KEY              NUMBER          GENERATED ALWAYS AS IDENTITY,
    SELLER_ID               VARCHAR2(50)    NOT NULL,
    ZIP_CODE_PREFIX         VARCHAR2(10),
    CITY                    VARCHAR2(100),
    STATE                   VARCHAR2(5),
    REGION                  VARCHAR2(30),
    LAT                     NUMBER(10,6),
    LNG                     NUMBER(10,6),
    IS_ACTIVE               CHAR(1)         DEFAULT 'Y',
    ETL_BATCH_ID            NUMBER,
    ETL_INSERT_DT           DATE            DEFAULT SYSDATE,
    ETL_UPDATE_DT           DATE,
    CONSTRAINT DIM_SELLER_PK PRIMARY KEY (SELLER_KEY),
    CONSTRAINT DIM_SELLER_BK UNIQUE (SELLER_ID)
);

-- ============================================================
-- DIMENSION: DIM_PRODUCT (SCD Type 2)
-- Source: STG_PRODUCTS + STG_CATEGORY_TRANSLATION
-- ============================================================
CREATE TABLE DWH.DIM_PRODUCT (
    PRODUCT_KEY             NUMBER          GENERATED ALWAYS AS IDENTITY,
    PRODUCT_ID              VARCHAR2(50)    NOT NULL,
    CATEGORY_NAME_PT        VARCHAR2(100),  -- Portuguese original
    CATEGORY_NAME_EN        VARCHAR2(100),  -- English translated
    CATEGORY_GROUP          VARCHAR2(50),   -- Derived high-level group
    PRODUCT_NAME_LENGTH     NUMBER,
    PRODUCT_DESC_LENGTH     NUMBER,
    PRODUCT_PHOTOS_QTY      NUMBER,
    WEIGHT_G                NUMBER,
    LENGTH_CM               NUMBER,
    HEIGHT_CM               NUMBER,
    WIDTH_CM                NUMBER,
    VOLUME_CM3              NUMBER GENERATED ALWAYS AS
                                (LENGTH_CM * HEIGHT_CM * WIDTH_CM) VIRTUAL,
    EFFECTIVE_FROM          DATE            NOT NULL,
    EFFECTIVE_TO            DATE,
    IS_CURRENT              CHAR(1)         DEFAULT 'Y',
    ETL_BATCH_ID            NUMBER,
    ETL_INSERT_DT           DATE            DEFAULT SYSDATE,
    CONSTRAINT DIM_PRODUCT_PK PRIMARY KEY (PRODUCT_KEY)
);
CREATE INDEX DIM_PROD_ID_IDX ON DWH.DIM_PRODUCT (PRODUCT_ID, IS_CURRENT);
CREATE INDEX DIM_PROD_CAT_IDX ON DWH.DIM_PRODUCT (CATEGORY_NAME_EN);

-- ============================================================
-- FACT: FACT_ORDERS
-- Grain: One row per ORDER
-- Source: STG_ORDERS + STG_ORDER_PAYMENTS
-- ============================================================
CREATE TABLE DWH.FACT_ORDERS (
    ORDER_FACT_KEY              NUMBER          GENERATED ALWAYS AS IDENTITY,
    ORDER_ID                    VARCHAR2(50)    NOT NULL,
    CUSTOMER_KEY                NUMBER          NOT NULL,
    ORDER_DATE_KEY              NUMBER          NOT NULL,   -- FK DIM_DATE
    APPROVED_DATE_KEY           NUMBER,
    DELIVERED_DATE_KEY          NUMBER,
    ESTIMATED_DELIVERY_DATE_KEY NUMBER,
    ORDER_STATUS                VARCHAR2(30),
    -- Payment Aggregates (rolled up from STG_ORDER_PAYMENTS)
    TOTAL_PAYMENT_VALUE         NUMBER(14,2),
    PAYMENT_INSTALLMENTS_MAX    NUMBER,
    PAYMENT_TYPE_PRIMARY        VARCHAR2(30),   -- Most used payment type
    NUM_PAYMENT_METHODS         NUMBER,
    -- Delivery Performance
    DAYS_TO_DELIVER             NUMBER,         -- Actual delivered - purchase
    DAYS_TO_APPROVE             NUMBER,         -- Approved - purchase
    DELIVERY_DELAY_DAYS         NUMBER,         -- Actual - estimated (negative = early)
    IS_LATE_DELIVERY            CHAR(1)         DEFAULT 'N',
    -- Review
    REVIEW_SCORE                NUMBER(1),
    HAS_REVIEW                  CHAR(1)         DEFAULT 'N',
    -- ETL
    ETL_BATCH_ID                NUMBER,
    ETL_INSERT_DT               DATE            DEFAULT SYSDATE,
    CONSTRAINT FACT_ORDERS_PK PRIMARY KEY (ORDER_FACT_KEY),
    CONSTRAINT FACT_ORD_CUST_FK  FOREIGN KEY (CUSTOMER_KEY) REFERENCES DWH.DIM_CUSTOMER (CUSTOMER_KEY),
    CONSTRAINT FACT_ORD_DATE_FK  FOREIGN KEY (ORDER_DATE_KEY) REFERENCES DWH.DIM_DATE (DATE_KEY)
);
CREATE INDEX FACT_ORD_ORDERID_IDX   ON DWH.FACT_ORDERS (ORDER_ID);
CREATE INDEX FACT_ORD_DATE_IDX      ON DWH.FACT_ORDERS (ORDER_DATE_KEY);
CREATE INDEX FACT_ORD_CUST_IDX      ON DWH.FACT_ORDERS (CUSTOMER_KEY);
CREATE INDEX FACT_ORD_STATUS_IDX    ON DWH.FACT_ORDERS (ORDER_STATUS);

-- ============================================================
-- FACT: FACT_ORDER_ITEMS
-- Grain: One row per ORDER LINE ITEM
-- Source: STG_ORDER_ITEMS + FACT_ORDERS + DIM_PRODUCT + DIM_SELLER
-- ============================================================
CREATE TABLE DWH.FACT_ORDER_ITEMS (
    ITEM_FACT_KEY           NUMBER          GENERATED ALWAYS AS IDENTITY,
    ORDER_ID                VARCHAR2(50)    NOT NULL,
    ORDER_ITEM_ID           NUMBER          NOT NULL,
    ORDER_DATE_KEY          NUMBER          NOT NULL,
    CUSTOMER_KEY            NUMBER          NOT NULL,
    PRODUCT_KEY             NUMBER          NOT NULL,
    SELLER_KEY              NUMBER          NOT NULL,
    -- Measures
    PRICE                   NUMBER(12,2),
    FREIGHT_VALUE           NUMBER(12,2),
    TOTAL_ITEM_VALUE        NUMBER(14,2)
                                GENERATED ALWAYS AS (PRICE + FREIGHT_VALUE) VIRTUAL,
    -- ETL
    ETL_BATCH_ID            NUMBER,
    ETL_INSERT_DT           DATE            DEFAULT SYSDATE,
    CONSTRAINT FACT_ITEMS_PK    PRIMARY KEY (ITEM_FACT_KEY),
    CONSTRAINT FACT_ITEMS_PROD_FK FOREIGN KEY (PRODUCT_KEY) REFERENCES DWH.DIM_PRODUCT (PRODUCT_KEY),
    CONSTRAINT FACT_ITEMS_SELL_FK FOREIGN KEY (SELLER_KEY)  REFERENCES DWH.DIM_SELLER (SELLER_KEY),
    CONSTRAINT FACT_ITEMS_CUST_FK FOREIGN KEY (CUSTOMER_KEY) REFERENCES DWH.DIM_CUSTOMER (CUSTOMER_KEY)
);
CREATE INDEX FACT_ITEMS_ORDER_IDX   ON DWH.FACT_ORDER_ITEMS (ORDER_ID);
CREATE INDEX FACT_ITEMS_DATE_IDX    ON DWH.FACT_ORDER_ITEMS (ORDER_DATE_KEY);
CREATE INDEX FACT_ITEMS_PROD_IDX    ON DWH.FACT_ORDER_ITEMS (PRODUCT_KEY);
CREATE INDEX FACT_ITEMS_SELL_IDX    ON DWH.FACT_ORDER_ITEMS (SELLER_KEY);

-- ============================================================
-- FACT: FACT_REVIEWS
-- Grain: One row per review
-- Source: STG_ORDER_REVIEWS
-- ============================================================
CREATE TABLE DWH.FACT_REVIEWS (
    REVIEW_FACT_KEY         NUMBER          GENERATED ALWAYS AS IDENTITY,
    REVIEW_ID               VARCHAR2(50),
    ORDER_ID                VARCHAR2(50),
    REVIEW_DATE_KEY         NUMBER,
    REVIEW_SCORE            NUMBER(1),
    SENTIMENT               VARCHAR2(10),   -- Derived: POSITIVE/NEUTRAL/NEGATIVE
    HAS_COMMENT_TITLE       CHAR(1)         DEFAULT 'N',
    HAS_COMMENT_MESSAGE     CHAR(1)         DEFAULT 'N',
    RESPONSE_DAYS           NUMBER,         -- answer_timestamp - creation_date
    ETL_BATCH_ID            NUMBER,
    ETL_INSERT_DT           DATE            DEFAULT SYSDATE,
    CONSTRAINT FACT_REVIEWS_PK PRIMARY KEY (REVIEW_FACT_KEY)
);
CREATE INDEX FACT_REV_ORDER_IDX  ON DWH.FACT_REVIEWS (ORDER_ID);
CREATE INDEX FACT_REV_DATE_IDX   ON DWH.FACT_REVIEWS (REVIEW_DATE_KEY);
CREATE INDEX FACT_REV_SCORE_IDX  ON DWH.FACT_REVIEWS (REVIEW_SCORE);

-- ============================================================
-- AGGREGATE: AGG_DAILY_SALES
-- Pre-aggregated for BI tool performance
-- Refreshed nightly by ETL
-- ============================================================
CREATE TABLE DWH.AGG_DAILY_SALES (
    AGG_KEY             NUMBER          GENERATED ALWAYS AS IDENTITY,
    DATE_KEY            NUMBER          NOT NULL,
    STATE               VARCHAR2(5),
    CATEGORY_EN         VARCHAR2(100),
    PAYMENT_TYPE        VARCHAR2(30),
    ORDER_STATUS        VARCHAR2(30),
    ORDER_COUNT         NUMBER          DEFAULT 0,
    ITEM_COUNT          NUMBER          DEFAULT 0,
    TOTAL_REVENUE       NUMBER(16,2)    DEFAULT 0,
    TOTAL_FREIGHT       NUMBER(16,2)    DEFAULT 0,
    AVG_REVIEW_SCORE    NUMBER(4,2),
    LATE_DELIVERY_COUNT NUMBER          DEFAULT 0,
    ETL_BATCH_ID        NUMBER,
    ETL_INSERT_DT       DATE            DEFAULT SYSDATE,
    CONSTRAINT AGG_DAILY_PK PRIMARY KEY (AGG_KEY)
);
CREATE INDEX AGG_DAILY_DATE_IDX  ON DWH.AGG_DAILY_SALES (DATE_KEY);
CREATE INDEX AGG_DAILY_STATE_IDX ON DWH.AGG_DAILY_SALES (STATE);

-- ============================================================
-- ETL CONTROL TABLES
-- ============================================================
CREATE TABLE ETL_CTRL.ETL_BATCH_LOG (
    BATCH_ID            NUMBER          GENERATED ALWAYS AS IDENTITY,
    BATCH_NAME          VARCHAR2(200)   NOT NULL,
    PIPELINE_NAME       VARCHAR2(100),
    START_DT            DATE,
    END_DT              DATE,
    STATUS              VARCHAR2(20),   -- RUNNING / SUCCESS / FAILED / PARTIAL
    TOTAL_RECORDS       NUMBER          DEFAULT 0,
    SUCCESS_RECORDS     NUMBER          DEFAULT 0,
    REJECTED_RECORDS    NUMBER          DEFAULT 0,
    ERROR_MESSAGE       VARCHAR2(4000),
    TRIGGERED_BY        VARCHAR2(100),
    CONSTRAINT ETL_BATCH_PK PRIMARY KEY (BATCH_ID)
);

CREATE TABLE ETL_CTRL.ETL_STEP_LOG (
    STEP_LOG_ID         NUMBER          GENERATED ALWAYS AS IDENTITY,
    BATCH_ID            NUMBER          NOT NULL,
    STEP_NAME           VARCHAR2(200),
    STEP_STATUS         VARCHAR2(20),
    RECORDS_IN          NUMBER          DEFAULT 0,
    RECORDS_OUT         NUMBER          DEFAULT 0,
    RECORDS_REJECTED    NUMBER          DEFAULT 0,
    START_DT            DATE,
    END_DT              DATE,
    DURATION_SECS       NUMBER GENERATED ALWAYS AS
                            (CASE WHEN END_DT IS NOT NULL
                             THEN ROUND((END_DT - START_DT)*86400,1) ELSE NULL END) VIRTUAL,
    ERROR_MESSAGE       VARCHAR2(4000),
    CONSTRAINT ETL_STEP_PK      PRIMARY KEY (STEP_LOG_ID),
    CONSTRAINT ETL_STEP_BATCH_FK FOREIGN KEY (BATCH_ID) REFERENCES ETL_CTRL.ETL_BATCH_LOG(BATCH_ID)
);

CREATE TABLE ETL_CTRL.ETL_WATERMARK (
    SOURCE_NAME         VARCHAR2(100)   NOT NULL,
    ENTITY_NAME         VARCHAR2(100)   NOT NULL,
    LAST_LOAD_DT        DATE,
    LAST_BATCH_ID       NUMBER,
    ETL_UPDATE_DT       DATE            DEFAULT SYSDATE,
    CONSTRAINT ETL_WM_PK PRIMARY KEY (SOURCE_NAME, ENTITY_NAME)
);

-- Seed watermarks for all Olist source files
INSERT INTO ETL_CTRL.ETL_WATERMARK (SOURCE_NAME, ENTITY_NAME, LAST_LOAD_DT)
VALUES ('OLIST_CSV', 'ORDERS',               TO_DATE('2015-01-01','YYYY-MM-DD'));
INSERT INTO ETL_CTRL.ETL_WATERMARK (SOURCE_NAME, ENTITY_NAME, LAST_LOAD_DT)
VALUES ('OLIST_CSV', 'ORDER_ITEMS',          TO_DATE('2015-01-01','YYYY-MM-DD'));
INSERT INTO ETL_CTRL.ETL_WATERMARK (SOURCE_NAME, ENTITY_NAME, LAST_LOAD_DT)
VALUES ('OLIST_CSV', 'ORDER_PAYMENTS',       TO_DATE('2015-01-01','YYYY-MM-DD'));
INSERT INTO ETL_CTRL.ETL_WATERMARK (SOURCE_NAME, ENTITY_NAME, LAST_LOAD_DT)
VALUES ('OLIST_CSV', 'ORDER_REVIEWS',        TO_DATE('2015-01-01','YYYY-MM-DD'));
INSERT INTO ETL_CTRL.ETL_WATERMARK (SOURCE_NAME, ENTITY_NAME, LAST_LOAD_DT)
VALUES ('OLIST_CSV', 'CUSTOMERS',            TO_DATE('2015-01-01','YYYY-MM-DD'));
INSERT INTO ETL_CTRL.ETL_WATERMARK (SOURCE_NAME, ENTITY_NAME, LAST_LOAD_DT)
VALUES ('OLIST_CSV', 'SELLERS',              TO_DATE('2015-01-01','YYYY-MM-DD'));
INSERT INTO ETL_CTRL.ETL_WATERMARK (SOURCE_NAME, ENTITY_NAME, LAST_LOAD_DT)
VALUES ('OLIST_CSV', 'PRODUCTS',             TO_DATE('2015-01-01','YYYY-MM-DD'));
INSERT INTO ETL_CTRL.ETL_WATERMARK (SOURCE_NAME, ENTITY_NAME, LAST_LOAD_DT)
VALUES ('OLIST_CSV', 'GEOLOCATION',          TO_DATE('2015-01-01','YYYY-MM-DD'));
INSERT INTO ETL_CTRL.ETL_WATERMARK (SOURCE_NAME, ENTITY_NAME, LAST_LOAD_DT)
VALUES ('OLIST_CSV', 'CATEGORY_TRANSLATION', TO_DATE('2015-01-01','YYYY-MM-DD'));
COMMIT;

-- ============================================================
-- POPULATE DIM_DATE (2015-01-01 to 2030-12-31)
-- ============================================================
DECLARE
    v_start DATE := DATE '2015-01-01';
    v_end   DATE := DATE '2030-12-31';
    v_date  DATE;
BEGIN
    v_date := v_start;
    LOOP
        EXIT WHEN v_date > v_end;
        INSERT INTO DWH.DIM_DATE (
            DATE_KEY, FULL_DATE, DAY_NUM, DAY_NAME,
            WEEK_NUM, MONTH_NUM, MONTH_NAME,
            QUARTER_NUM, QUARTER_NAME, YEAR_NUM,
            IS_WEEKEND
        ) VALUES (
            TO_NUMBER(TO_CHAR(v_date,'YYYYMMDD')),
            v_date,
            TO_NUMBER(TO_CHAR(v_date,'DD')),
            TO_CHAR(v_date,'Day'),
            TO_NUMBER(TO_CHAR(v_date,'IW')),
            TO_NUMBER(TO_CHAR(v_date,'MM')),
            TO_CHAR(v_date,'Month'),
            TO_NUMBER(TO_CHAR(v_date,'Q')),
            'Q' || TO_CHAR(v_date,'Q'),
            TO_NUMBER(TO_CHAR(v_date,'YYYY')),
            CASE WHEN TO_CHAR(v_date,'D') IN ('1','7') THEN 'Y' ELSE 'N' END
        );
        v_date := v_date + 1;
    END LOOP;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('DIM_DATE populated: ' || SQL%ROWCOUNT || ' rows');
END;
/
