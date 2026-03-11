-- =============================================================================
-- PROJECT   : Olist Brazilian E-Commerce ETL Data Pipeline
-- FILE      : 02_staging_ddl.sql
-- PURPOSE   : Staging layer — exact mapping of all 9 Olist CSV files
-- SCHEMA    : STG
-- DATASET   : https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
-- =============================================================================
-- CSV FILES COVERED:
--   olist_orders_dataset.csv
--   olist_order_items_dataset.csv
--   olist_order_payments_dataset.csv
--   olist_order_reviews_dataset.csv
--   olist_customers_dataset.csv
--   olist_sellers_dataset.csv
--   olist_products_dataset.csv
--   olist_geolocation_dataset.csv
--   product_category_name_translation.csv
-- =============================================================================

-- Drop existing staging tables (for re-runs)
BEGIN
    FOR t IN (SELECT table_name FROM all_tables WHERE owner = 'STG') LOOP
        EXECUTE IMMEDIATE 'DROP TABLE STG.' || t.table_name || ' CASCADE CONSTRAINTS';
    END LOOP;
END;
/

-- -----------------------------------------------------------------------
-- STG_ORDERS
-- Source: olist_orders_dataset.csv
-- Columns: order_id, customer_id, order_status, order_purchase_timestamp,
--          order_approved_at, order_delivered_carrier_date,
--          order_delivered_customer_date, order_estimated_delivery_date
-- -----------------------------------------------------------------------
CREATE TABLE STG.STG_ORDERS (
    ORDER_ID                        VARCHAR2(50),
    CUSTOMER_ID                     VARCHAR2(50),
    ORDER_STATUS                    VARCHAR2(30),
    ORDER_PURCHASE_TIMESTAMP        VARCHAR2(30),
    ORDER_APPROVED_AT               VARCHAR2(30),
    ORDER_DELIVERED_CARRIER_DATE    VARCHAR2(30),
    ORDER_DELIVERED_CUSTOMER_DATE   VARCHAR2(30),
    ORDER_ESTIMATED_DELIVERY_DATE   VARCHAR2(30),
    -- ETL Control Columns
    ETL_BATCH_ID                    NUMBER,
    ETL_INSERT_DT                   DATE DEFAULT SYSDATE,
    ETL_SOURCE_FILE                 VARCHAR2(200) DEFAULT 'olist_orders_dataset.csv'
);

-- -----------------------------------------------------------------------
-- STG_ORDER_ITEMS
-- Source: olist_order_items_dataset.csv
-- Columns: order_id, order_item_id, product_id, seller_id,
--          shipping_limit_date, price, freight_value
-- -----------------------------------------------------------------------
CREATE TABLE STG.STG_ORDER_ITEMS (
    ORDER_ID                VARCHAR2(50),
    ORDER_ITEM_ID           NUMBER,         -- Sequential item number within order
    PRODUCT_ID              VARCHAR2(50),
    SELLER_ID               VARCHAR2(50),
    SHIPPING_LIMIT_DATE     VARCHAR2(30),
    PRICE                   VARCHAR2(20),
    FREIGHT_VALUE           VARCHAR2(20),
    ETL_BATCH_ID            NUMBER,
    ETL_INSERT_DT           DATE DEFAULT SYSDATE,
    ETL_SOURCE_FILE         VARCHAR2(200) DEFAULT 'olist_order_items_dataset.csv'
);

-- -----------------------------------------------------------------------
-- STG_ORDER_PAYMENTS
-- Source: olist_order_payments_dataset.csv
-- Columns: order_id, payment_sequential, payment_type,
--          payment_installments, payment_value
-- -----------------------------------------------------------------------
CREATE TABLE STG.STG_ORDER_PAYMENTS (
    ORDER_ID                VARCHAR2(50),
    PAYMENT_SEQUENTIAL      NUMBER,         -- Multiple payments per order possible
    PAYMENT_TYPE            VARCHAR2(30),   -- credit_card, boleto, voucher, debit_card
    PAYMENT_INSTALLMENTS    NUMBER,
    PAYMENT_VALUE           VARCHAR2(20),
    ETL_BATCH_ID            NUMBER,
    ETL_INSERT_DT           DATE DEFAULT SYSDATE,
    ETL_SOURCE_FILE         VARCHAR2(200) DEFAULT 'olist_order_payments_dataset.csv'
);

-- -----------------------------------------------------------------------
-- STG_ORDER_REVIEWS
-- Source: olist_order_reviews_dataset.csv
-- Columns: review_id, order_id, review_score, review_comment_title,
--          review_comment_message, review_creation_date, review_answer_timestamp
-- -----------------------------------------------------------------------
CREATE TABLE STG.STG_ORDER_REVIEWS (
    REVIEW_ID               VARCHAR2(50),
    ORDER_ID                VARCHAR2(50),
    REVIEW_SCORE            NUMBER(1),      -- 1 to 5 stars
    REVIEW_COMMENT_TITLE    VARCHAR2(500),
    REVIEW_COMMENT_MESSAGE  CLOB,
    REVIEW_CREATION_DATE    VARCHAR2(30),
    REVIEW_ANSWER_TIMESTAMP VARCHAR2(30),
    ETL_BATCH_ID            NUMBER,
    ETL_INSERT_DT           DATE DEFAULT SYSDATE,
    ETL_SOURCE_FILE         VARCHAR2(200) DEFAULT 'olist_order_reviews_dataset.csv'
);

-- -----------------------------------------------------------------------
-- STG_CUSTOMERS
-- Source: olist_customers_dataset.csv
-- Columns: customer_id, customer_unique_id, customer_zip_code_prefix,
--          customer_city, customer_state
-- -----------------------------------------------------------------------
CREATE TABLE STG.STG_CUSTOMERS (
    CUSTOMER_ID             VARCHAR2(50),
    CUSTOMER_UNIQUE_ID      VARCHAR2(50),   -- True customer identity (repeat purchases)
    CUSTOMER_ZIP_CODE_PREFIX VARCHAR2(10),
    CUSTOMER_CITY           VARCHAR2(100),
    CUSTOMER_STATE          VARCHAR2(5),
    ETL_BATCH_ID            NUMBER,
    ETL_INSERT_DT           DATE DEFAULT SYSDATE,
    ETL_SOURCE_FILE         VARCHAR2(200) DEFAULT 'olist_customers_dataset.csv'
);

-- -----------------------------------------------------------------------
-- STG_SELLERS
-- Source: olist_sellers_dataset.csv
-- Columns: seller_id, seller_zip_code_prefix, seller_city, seller_state
-- -----------------------------------------------------------------------
CREATE TABLE STG.STG_SELLERS (
    SELLER_ID               VARCHAR2(50),
    SELLER_ZIP_CODE_PREFIX  VARCHAR2(10),
    SELLER_CITY             VARCHAR2(100),
    SELLER_STATE            VARCHAR2(5),
    ETL_BATCH_ID            NUMBER,
    ETL_INSERT_DT           DATE DEFAULT SYSDATE,
    ETL_SOURCE_FILE         VARCHAR2(200) DEFAULT 'olist_sellers_dataset.csv'
);

-- -----------------------------------------------------------------------
-- STG_PRODUCTS
-- Source: olist_products_dataset.csv
-- Columns: product_id, product_category_name, product_name_lenght,
--          product_description_lenght, product_photos_qty,
--          product_weight_g, product_length_cm, product_height_cm,
--          product_width_cm
-- -----------------------------------------------------------------------
CREATE TABLE STG.STG_PRODUCTS (
    PRODUCT_ID                      VARCHAR2(50),
    PRODUCT_CATEGORY_NAME           VARCHAR2(100),  -- Portuguese; joined with translation table
    PRODUCT_NAME_LENGHT             NUMBER,         -- Note: Olist CSV has typo "lenght"
    PRODUCT_DESCRIPTION_LENGHT      NUMBER,
    PRODUCT_PHOTOS_QTY              NUMBER,
    PRODUCT_WEIGHT_G                NUMBER,
    PRODUCT_LENGTH_CM               NUMBER,
    PRODUCT_HEIGHT_CM               NUMBER,
    PRODUCT_WIDTH_CM                NUMBER,
    ETL_BATCH_ID                    NUMBER,
    ETL_INSERT_DT                   DATE DEFAULT SYSDATE,
    ETL_SOURCE_FILE                 VARCHAR2(200) DEFAULT 'olist_products_dataset.csv'
);

-- -----------------------------------------------------------------------
-- STG_GEOLOCATION
-- Source: olist_geolocation_dataset.csv
-- Columns: geolocation_zip_code_prefix, geolocation_lat,
--          geolocation_lng, geolocation_city, geolocation_state
-- NOTE: This CSV has ~1M rows (multiple lat/lng per zip)
-- -----------------------------------------------------------------------
CREATE TABLE STG.STG_GEOLOCATION (
    GEOLOCATION_ZIP_CODE_PREFIX     VARCHAR2(10),
    GEOLOCATION_LAT                 VARCHAR2(30),
    GEOLOCATION_LNG                 VARCHAR2(30),
    GEOLOCATION_CITY                VARCHAR2(100),
    GEOLOCATION_STATE               VARCHAR2(5),
    ETL_BATCH_ID                    NUMBER,
    ETL_INSERT_DT                   DATE DEFAULT SYSDATE,
    ETL_SOURCE_FILE                 VARCHAR2(200) DEFAULT 'olist_geolocation_dataset.csv'
);

-- -----------------------------------------------------------------------
-- STG_CATEGORY_TRANSLATION
-- Source: product_category_name_translation.csv
-- Columns: product_category_name, product_category_name_english
-- -----------------------------------------------------------------------
CREATE TABLE STG.STG_CATEGORY_TRANSLATION (
    PRODUCT_CATEGORY_NAME           VARCHAR2(100),  -- Portuguese
    PRODUCT_CATEGORY_NAME_ENGLISH   VARCHAR2(100),  -- English translation
    ETL_BATCH_ID                    NUMBER,
    ETL_INSERT_DT                   DATE DEFAULT SYSDATE,
    ETL_SOURCE_FILE                 VARCHAR2(200) DEFAULT 'product_category_name_translation.csv'
);

-- -----------------------------------------------------------------------
-- STG_REJECTED_RECORDS — quarantine for failed DQ checks
-- -----------------------------------------------------------------------
CREATE TABLE STG.STG_REJECTED_RECORDS (
    REJECT_ID       NUMBER GENERATED ALWAYS AS IDENTITY,
    SOURCE_TABLE    VARCHAR2(100),
    SOURCE_KEY      VARCHAR2(500),
    REJECT_REASON   VARCHAR2(1000),
    RAW_DATA        CLOB,
    ETL_BATCH_ID    NUMBER,
    ETL_INSERT_DT   DATE DEFAULT SYSDATE,
    CONSTRAINT STG_REJECTED_PK PRIMARY KEY (REJECT_ID)
);

COMMIT;
