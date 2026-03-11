-- =============================================================================
-- PROJECT   : Olist Brazilian E-Commerce ETL Data Pipeline
-- FILE      : packages/02_pkg_transform.sql
-- PURPOSE   : Transform & Load — STG → DWH
--             Dimensions: DIM_CUSTOMER, DIM_SELLER, DIM_PRODUCT
--             Facts:      FACT_ORDERS, FACT_ORDER_ITEMS, FACT_REVIEWS
--             Aggregate:  AGG_DAILY_SALES
-- SCHEMA    : ETL_CTRL
-- =============================================================================

CREATE OR REPLACE PACKAGE ETL_CTRL.PKG_TRANSFORM AS

    -- Data Quality
    PROCEDURE validate_staging(p_batch_id IN NUMBER);

    -- Dimensions
    PROCEDURE load_dim_customer(p_batch_id IN NUMBER);
    PROCEDURE load_dim_seller(p_batch_id IN NUMBER);
    PROCEDURE load_dim_product(p_batch_id IN NUMBER);

    -- Facts
    PROCEDURE load_fact_orders(p_batch_id IN NUMBER);
    PROCEDURE load_fact_order_items(p_batch_id IN NUMBER);
    PROCEDURE load_fact_reviews(p_batch_id IN NUMBER);

    -- Aggregate
    PROCEDURE refresh_agg_daily_sales(p_batch_id IN NUMBER);

    -- Master
    PROCEDURE run_all(p_batch_id IN NUMBER);

END PKG_TRANSFORM;
/

CREATE OR REPLACE PACKAGE BODY ETL_CTRL.PKG_TRANSFORM AS

    -- ================================================================
    -- PRIVATE: Brazilian state name lookup
    -- ================================================================
    FUNCTION get_state_name(p_abbr IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        RETURN CASE UPPER(p_abbr)
            WHEN 'AC' THEN 'Acre'            WHEN 'AL' THEN 'Alagoas'
            WHEN 'AM' THEN 'Amazonas'        WHEN 'AP' THEN 'Amapa'
            WHEN 'BA' THEN 'Bahia'           WHEN 'CE' THEN 'Ceara'
            WHEN 'DF' THEN 'Distrito Federal' WHEN 'ES' THEN 'Espirito Santo'
            WHEN 'GO' THEN 'Goias'           WHEN 'MA' THEN 'Maranhao'
            WHEN 'MG' THEN 'Minas Gerais'    WHEN 'MS' THEN 'Mato Grosso do Sul'
            WHEN 'MT' THEN 'Mato Grosso'     WHEN 'PA' THEN 'Para'
            WHEN 'PB' THEN 'Paraiba'         WHEN 'PE' THEN 'Pernambuco'
            WHEN 'PI' THEN 'Piaui'           WHEN 'PR' THEN 'Parana'
            WHEN 'RJ' THEN 'Rio de Janeiro'  WHEN 'RN' THEN 'Rio Grande do Norte'
            WHEN 'RO' THEN 'Rondonia'        WHEN 'RR' THEN 'Roraima'
            WHEN 'RS' THEN 'Rio Grande do Sul' WHEN 'SC' THEN 'Santa Catarina'
            WHEN 'SE' THEN 'Sergipe'         WHEN 'SP' THEN 'Sao Paulo'
            WHEN 'TO' THEN 'Tocantins'
            ELSE 'Unknown'
        END;
    END get_state_name;

    -- ================================================================
    -- PRIVATE: Derive Brazilian region from state code
    -- ================================================================
    FUNCTION get_region(p_state IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        RETURN CASE UPPER(p_state)
            WHEN 'AC' THEN 'North'  WHEN 'AM' THEN 'North'  WHEN 'AP' THEN 'North'
            WHEN 'PA' THEN 'North'  WHEN 'RO' THEN 'North'  WHEN 'RR' THEN 'North'
            WHEN 'TO' THEN 'North'
            WHEN 'AL' THEN 'Northeast' WHEN 'BA' THEN 'Northeast' WHEN 'CE' THEN 'Northeast'
            WHEN 'MA' THEN 'Northeast' WHEN 'PB' THEN 'Northeast' WHEN 'PE' THEN 'Northeast'
            WHEN 'PI' THEN 'Northeast' WHEN 'RN' THEN 'Northeast' WHEN 'SE' THEN 'Northeast'
            WHEN 'DF' THEN 'Central-West' WHEN 'GO' THEN 'Central-West'
            WHEN 'MS' THEN 'Central-West' WHEN 'MT' THEN 'Central-West'
            WHEN 'ES' THEN 'Southeast' WHEN 'MG' THEN 'Southeast'
            WHEN 'RJ' THEN 'Southeast' WHEN 'SP' THEN 'Southeast'
            WHEN 'PR' THEN 'South'  WHEN 'RS' THEN 'South'  WHEN 'SC' THEN 'South'
            ELSE 'Unknown'
        END;
    END get_region;

    -- ================================================================
    -- PRIVATE: Safe date key from varchar timestamp
    -- ================================================================
    FUNCTION to_date_key(p_str IN VARCHAR2) RETURN NUMBER IS
        v_dt DATE;
    BEGIN
        IF p_str IS NULL THEN RETURN NULL; END IF;
        v_dt := TO_DATE(SUBSTR(p_str,1,10), 'YYYY-MM-DD');
        RETURN TO_NUMBER(TO_CHAR(v_dt,'YYYYMMDD'));
    EXCEPTION WHEN OTHERS THEN RETURN NULL;
    END to_date_key;

    -- ================================================================
    -- DATA QUALITY VALIDATION
    -- ================================================================
    PROCEDURE validate_staging(p_batch_id IN NUMBER) IS
        v_step_id NUMBER;
        v_rej     NUMBER := 0;
    BEGIN
        v_step_id := ETL_CTRL.PKG_ETL_LOGGER.start_step(p_batch_id, 'DQ_VALIDATE_STAGING');

        -- NULL ORDER_ID in orders
        FOR r IN (SELECT 'NULL ORDER_ID' rz FROM STG.STG_ORDERS
                  WHERE ETL_BATCH_ID = p_batch_id AND ORDER_ID IS NULL)
        LOOP
            ETL_CTRL.PKG_ETL_LOGGER.log_reject(p_batch_id,'STG_ORDERS','NULL','NULL ORDER_ID');
            v_rej := v_rej + 1;
        END LOOP;

        -- Orders with invalid status
        FOR r IN (SELECT ORDER_ID FROM STG.STG_ORDERS
                  WHERE ETL_BATCH_ID = p_batch_id
                    AND ORDER_STATUS NOT IN ('delivered','shipped','canceled',
                        'invoiced','processing','created','approved','unavailable'))
        LOOP
            ETL_CTRL.PKG_ETL_LOGGER.log_reject(p_batch_id,'STG_ORDERS',r.ORDER_ID,'Invalid ORDER_STATUS');
            v_rej := v_rej + 1;
        END LOOP;

        -- Order items with negative price
        FOR r IN (SELECT ORDER_ID||'-'||ORDER_ITEM_ID AS k FROM STG.STG_ORDER_ITEMS
                  WHERE ETL_BATCH_ID = p_batch_id
                    AND (TO_NUMBER(PRICE) < 0 OR TO_NUMBER(FREIGHT_VALUE) < 0))
        LOOP
            ETL_CTRL.PKG_ETL_LOGGER.log_reject(p_batch_id,'STG_ORDER_ITEMS',r.k,'Negative price/freight');
            v_rej := v_rej + 1;
        END LOOP;

        -- Reviews with invalid score
        FOR r IN (SELECT REVIEW_ID FROM STG.STG_ORDER_REVIEWS
                  WHERE ETL_BATCH_ID = p_batch_id
                    AND (REVIEW_SCORE NOT BETWEEN 1 AND 5))
        LOOP
            ETL_CTRL.PKG_ETL_LOGGER.log_reject(p_batch_id,'STG_ORDER_REVIEWS',r.REVIEW_ID,'Score not 1-5');
            v_rej := v_rej + 1;
        END LOOP;

        ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'SUCCESS',0,0,v_rej);
        DBMS_OUTPUT.PUT_LINE('DQ complete. Rejected: ' || v_rej);
    EXCEPTION
        WHEN OTHERS THEN
            ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'FAILED',p_err=>SQLERRM);
            RAISE;
    END validate_staging;

    -- ================================================================
    -- DIM_CUSTOMER — SCD Type 1
    -- ================================================================
    PROCEDURE load_dim_customer(p_batch_id IN NUMBER) IS
        v_step_id NUMBER;
        v_cnt     NUMBER;
    BEGIN
        v_step_id := ETL_CTRL.PKG_ETL_LOGGER.start_step(p_batch_id, 'LOAD_DIM_CUSTOMER');

        MERGE INTO DWH.DIM_CUSTOMER tgt
        USING (
            SELECT
                c.CUSTOMER_UNIQUE_ID,
                c.CUSTOMER_ID,
                c.CUSTOMER_ZIP_CODE_PREFIX      AS ZIP_CODE_PREFIX,
                INITCAP(c.CUSTOMER_CITY)        AS CITY,
                UPPER(c.CUSTOMER_STATE)         AS STATE,
                get_state_name(c.CUSTOMER_STATE) AS STATE_NAME,
                get_region(c.CUSTOMER_STATE)     AS REGION,
                TO_NUMBER(REPLACE(g.GEOLOCATION_LAT,',','.')) AS LAT,
                TO_NUMBER(REPLACE(g.GEOLOCATION_LNG,',','.')) AS LNG,
                p_batch_id                       AS ETL_BATCH_ID,
                ROW_NUMBER() OVER (
                    PARTITION BY c.CUSTOMER_UNIQUE_ID
                    ORDER BY c.ETL_INSERT_DT DESC
                ) AS RN
            FROM STG.STG_CUSTOMERS c
            -- Join to geolocation: use average lat/lng per zip code
            LEFT JOIN (
                SELECT GEOLOCATION_ZIP_CODE_PREFIX,
                       AVG(TO_NUMBER(REPLACE(GEOLOCATION_LAT,',','.'))) AS GEOLOCATION_LAT,
                       AVG(TO_NUMBER(REPLACE(GEOLOCATION_LNG,',','.'))) AS GEOLOCATION_LNG
                FROM STG.STG_GEOLOCATION
                WHERE ETL_BATCH_ID = p_batch_id
                GROUP BY GEOLOCATION_ZIP_CODE_PREFIX
            ) g ON g.GEOLOCATION_ZIP_CODE_PREFIX = c.CUSTOMER_ZIP_CODE_PREFIX
            WHERE c.ETL_BATCH_ID = p_batch_id
        ) src
        ON (tgt.CUSTOMER_UNIQUE_ID = src.CUSTOMER_UNIQUE_ID AND src.RN = 1)
        WHEN MATCHED THEN UPDATE SET
            tgt.CUSTOMER_ID     = src.CUSTOMER_ID,
            tgt.ZIP_CODE_PREFIX = src.ZIP_CODE_PREFIX,
            tgt.CITY            = src.CITY,
            tgt.STATE           = src.STATE,
            tgt.STATE_NAME      = src.STATE_NAME,
            tgt.REGION          = src.REGION,
            tgt.LAT             = src.LAT,
            tgt.LNG             = src.LNG,
            tgt.ETL_BATCH_ID    = src.ETL_BATCH_ID,
            tgt.ETL_UPDATE_DT   = SYSDATE
        WHEN NOT MATCHED THEN INSERT (
            CUSTOMER_UNIQUE_ID, CUSTOMER_ID, ZIP_CODE_PREFIX, CITY,
            STATE, STATE_NAME, REGION, LAT, LNG, IS_ACTIVE, ETL_BATCH_ID, ETL_INSERT_DT
        ) VALUES (
            src.CUSTOMER_UNIQUE_ID, src.CUSTOMER_ID, src.ZIP_CODE_PREFIX, src.CITY,
            src.STATE, src.STATE_NAME, src.REGION, src.LAT, src.LNG,
            'Y', src.ETL_BATCH_ID, SYSDATE
        );

        v_cnt := SQL%ROWCOUNT;
        COMMIT;
        ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'SUCCESS',v_cnt,v_cnt);
        ETL_CTRL.PKG_ETL_LOGGER.set_watermark('OLIST_CSV','CUSTOMERS',SYSDATE,p_batch_id);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'FAILED',p_err=>SQLERRM);
            RAISE;
    END load_dim_customer;

    -- ================================================================
    -- DIM_SELLER — SCD Type 1
    -- ================================================================
    PROCEDURE load_dim_seller(p_batch_id IN NUMBER) IS
        v_step_id NUMBER;
        v_cnt     NUMBER;
    BEGIN
        v_step_id := ETL_CTRL.PKG_ETL_LOGGER.start_step(p_batch_id, 'LOAD_DIM_SELLER');

        MERGE INTO DWH.DIM_SELLER tgt
        USING (
            SELECT
                s.SELLER_ID,
                s.SELLER_ZIP_CODE_PREFIX        AS ZIP_CODE_PREFIX,
                INITCAP(s.SELLER_CITY)          AS CITY,
                UPPER(s.SELLER_STATE)           AS STATE,
                get_region(s.SELLER_STATE)      AS REGION,
                AVG(TO_NUMBER(REPLACE(g.GEOLOCATION_LAT,',','.'))) AS LAT,
                AVG(TO_NUMBER(REPLACE(g.GEOLOCATION_LNG,',','.'))) AS LNG,
                p_batch_id AS ETL_BATCH_ID
            FROM STG.STG_SELLERS s
            LEFT JOIN STG.STG_GEOLOCATION g
                ON g.GEOLOCATION_ZIP_CODE_PREFIX = s.SELLER_ZIP_CODE_PREFIX
               AND g.ETL_BATCH_ID = p_batch_id
            WHERE s.ETL_BATCH_ID = p_batch_id
            GROUP BY s.SELLER_ID, s.SELLER_ZIP_CODE_PREFIX, s.SELLER_CITY, s.SELLER_STATE
        ) src
        ON (tgt.SELLER_ID = src.SELLER_ID)
        WHEN MATCHED THEN UPDATE SET
            tgt.ZIP_CODE_PREFIX = src.ZIP_CODE_PREFIX,
            tgt.CITY            = src.CITY,
            tgt.STATE           = src.STATE,
            tgt.REGION          = src.REGION,
            tgt.LAT             = src.LAT,
            tgt.LNG             = src.LNG,
            tgt.ETL_BATCH_ID    = src.ETL_BATCH_ID,
            tgt.ETL_UPDATE_DT   = SYSDATE
        WHEN NOT MATCHED THEN INSERT (
            SELLER_ID, ZIP_CODE_PREFIX, CITY, STATE, REGION, LAT, LNG,
            IS_ACTIVE, ETL_BATCH_ID, ETL_INSERT_DT
        ) VALUES (
            src.SELLER_ID, src.ZIP_CODE_PREFIX, src.CITY, src.STATE, src.REGION,
            src.LAT, src.LNG, 'Y', src.ETL_BATCH_ID, SYSDATE
        );

        v_cnt := SQL%ROWCOUNT;
        COMMIT;
        ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'SUCCESS',v_cnt,v_cnt);
        ETL_CTRL.PKG_ETL_LOGGER.set_watermark('OLIST_CSV','SELLERS',SYSDATE,p_batch_id);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'FAILED',p_err=>SQLERRM);
            RAISE;
    END load_dim_seller;

    -- ================================================================
    -- DIM_PRODUCT — SCD Type 2
    -- ================================================================
    PROCEDURE load_dim_product(p_batch_id IN NUMBER) IS
        v_step_id NUMBER;
        v_cnt     NUMBER;
    BEGIN
        v_step_id := ETL_CTRL.PKG_ETL_LOGGER.start_step(p_batch_id, 'LOAD_DIM_PRODUCT');

        -- Expire changed records
        UPDATE DWH.DIM_PRODUCT tgt
        SET EFFECTIVE_TO = TRUNC(SYSDATE) - 1, IS_CURRENT = 'N'
        WHERE IS_CURRENT = 'Y'
          AND PRODUCT_ID IN (
              SELECT s.PRODUCT_ID FROM STG.STG_PRODUCTS s
              JOIN STG.STG_CATEGORY_TRANSLATION t
                ON t.PRODUCT_CATEGORY_NAME = s.PRODUCT_CATEGORY_NAME
                AND t.ETL_BATCH_ID = s.ETL_BATCH_ID
              WHERE s.ETL_BATCH_ID = p_batch_id
                AND (NVL(tgt.CATEGORY_NAME_EN,'~') <>
                     NVL(t.PRODUCT_CATEGORY_NAME_ENGLISH,'~'))
          );

        -- Insert new/changed products
        INSERT INTO DWH.DIM_PRODUCT (
            PRODUCT_ID, CATEGORY_NAME_PT, CATEGORY_NAME_EN, CATEGORY_GROUP,
            PRODUCT_NAME_LENGTH, PRODUCT_DESC_LENGTH, PRODUCT_PHOTOS_QTY,
            WEIGHT_G, LENGTH_CM, HEIGHT_CM, WIDTH_CM,
            EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT, ETL_BATCH_ID, ETL_INSERT_DT
        )
        SELECT
            s.PRODUCT_ID,
            s.PRODUCT_CATEGORY_NAME,
            NVL(t.PRODUCT_CATEGORY_NAME_ENGLISH, s.PRODUCT_CATEGORY_NAME),
            -- Derive high-level category group
            CASE
                WHEN t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%fashion%'       THEN 'Fashion & Apparel'
                WHEN t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%electronics%'   THEN 'Electronics'
                WHEN t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%health%'
                  OR t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%beauty%'        THEN 'Health & Beauty'
                WHEN t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%sports%'        THEN 'Sports & Leisure'
                WHEN t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%furniture%'
                  OR t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%home%'
                  OR t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%bed%'           THEN 'Home & Furniture'
                WHEN t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%toy%'
                  OR t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%baby%'          THEN 'Toys & Baby'
                WHEN t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%book%'          THEN 'Books & Media'
                WHEN t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%food%'
                  OR t.PRODUCT_CATEGORY_NAME_ENGLISH LIKE '%drink%'         THEN 'Food & Beverages'
                ELSE 'Other'
            END,
            s.PRODUCT_NAME_LENGHT,
            s.PRODUCT_DESCRIPTION_LENGHT,
            s.PRODUCT_PHOTOS_QTY,
            s.PRODUCT_WEIGHT_G,
            s.PRODUCT_LENGTH_CM,
            s.PRODUCT_HEIGHT_CM,
            s.PRODUCT_WIDTH_CM,
            TRUNC(SYSDATE),
            NULL,
            'Y',
            p_batch_id,
            SYSDATE
        FROM STG.STG_PRODUCTS s
        LEFT JOIN STG.STG_CATEGORY_TRANSLATION t
            ON t.PRODUCT_CATEGORY_NAME = s.PRODUCT_CATEGORY_NAME
           AND t.ETL_BATCH_ID = p_batch_id
        WHERE s.ETL_BATCH_ID = p_batch_id
          AND NOT EXISTS (
              SELECT 1 FROM DWH.DIM_PRODUCT p2
              WHERE p2.PRODUCT_ID = s.PRODUCT_ID AND p2.IS_CURRENT = 'Y'
          );

        v_cnt := SQL%ROWCOUNT;
        COMMIT;
        ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'SUCCESS',v_cnt,v_cnt);
        ETL_CTRL.PKG_ETL_LOGGER.set_watermark('OLIST_CSV','PRODUCTS',SYSDATE,p_batch_id);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'FAILED',p_err=>SQLERRM);
            RAISE;
    END load_dim_product;

    -- ================================================================
    -- FACT_ORDERS — one row per order, with payment rollup + review
    -- ================================================================
    PROCEDURE load_fact_orders(p_batch_id IN NUMBER) IS
        v_step_id NUMBER;
        v_cnt     NUMBER;
    BEGIN
        v_step_id := ETL_CTRL.PKG_ETL_LOGGER.start_step(p_batch_id, 'LOAD_FACT_ORDERS');

        INSERT INTO DWH.FACT_ORDERS (
            ORDER_ID, CUSTOMER_KEY,
            ORDER_DATE_KEY, APPROVED_DATE_KEY,
            DELIVERED_DATE_KEY, ESTIMATED_DELIVERY_DATE_KEY,
            ORDER_STATUS,
            TOTAL_PAYMENT_VALUE, PAYMENT_INSTALLMENTS_MAX,
            PAYMENT_TYPE_PRIMARY, NUM_PAYMENT_METHODS,
            DAYS_TO_DELIVER, DAYS_TO_APPROVE, DELIVERY_DELAY_DAYS,
            IS_LATE_DELIVERY,
            REVIEW_SCORE, HAS_REVIEW,
            ETL_BATCH_ID, ETL_INSERT_DT
        )
        SELECT
            o.ORDER_ID,
            c.CUSTOMER_KEY,
            to_date_key(o.ORDER_PURCHASE_TIMESTAMP),
            to_date_key(o.ORDER_APPROVED_AT),
            to_date_key(o.ORDER_DELIVERED_CUSTOMER_DATE),
            to_date_key(o.ORDER_ESTIMATED_DELIVERY_DATE),
            UPPER(o.ORDER_STATUS),
            -- Payment rollup
            NVL(p.TOTAL_PAYMENT,0),
            NVL(p.MAX_INSTALLMENTS,1),
            p.PRIMARY_PAYMENT_TYPE,
            NVL(p.NUM_METHODS,1),
            -- Delivery KPIs
            CASE WHEN o.ORDER_DELIVERED_CUSTOMER_DATE IS NOT NULL
                 THEN ROUND(TO_DATE(SUBSTR(o.ORDER_DELIVERED_CUSTOMER_DATE,1,10),'YYYY-MM-DD')
                            - TO_DATE(SUBSTR(o.ORDER_PURCHASE_TIMESTAMP,1,10),'YYYY-MM-DD'))
            END,
            CASE WHEN o.ORDER_APPROVED_AT IS NOT NULL
                 THEN ROUND(TO_DATE(SUBSTR(o.ORDER_APPROVED_AT,1,10),'YYYY-MM-DD')
                            - TO_DATE(SUBSTR(o.ORDER_PURCHASE_TIMESTAMP,1,10),'YYYY-MM-DD'))
            END,
            CASE WHEN o.ORDER_DELIVERED_CUSTOMER_DATE IS NOT NULL
                      AND o.ORDER_ESTIMATED_DELIVERY_DATE IS NOT NULL
                 THEN ROUND(TO_DATE(SUBSTR(o.ORDER_DELIVERED_CUSTOMER_DATE,1,10),'YYYY-MM-DD')
                            - TO_DATE(SUBSTR(o.ORDER_ESTIMATED_DELIVERY_DATE,1,10),'YYYY-MM-DD'))
            END,
            CASE WHEN o.ORDER_DELIVERED_CUSTOMER_DATE IS NOT NULL
                      AND o.ORDER_ESTIMATED_DELIVERY_DATE IS NOT NULL
                      AND TO_DATE(SUBSTR(o.ORDER_DELIVERED_CUSTOMER_DATE,1,10),'YYYY-MM-DD')
                          > TO_DATE(SUBSTR(o.ORDER_ESTIMATED_DELIVERY_DATE,1,10),'YYYY-MM-DD')
                 THEN 'Y' ELSE 'N'
            END,
            -- Review
            r.REVIEW_SCORE,
            CASE WHEN r.REVIEW_ID IS NOT NULL THEN 'Y' ELSE 'N' END,
            p_batch_id,
            SYSDATE
        FROM STG.STG_ORDERS o
        -- Customer dimension lookup
        JOIN DWH.DIM_CUSTOMER c
            ON c.CUSTOMER_ID = o.CUSTOMER_ID
        -- Payment aggregate subquery
        LEFT JOIN (
            SELECT
                ORDER_ID,
                SUM(TO_NUMBER(PAYMENT_VALUE))   AS TOTAL_PAYMENT,
                MAX(PAYMENT_INSTALLMENTS)        AS MAX_INSTALLMENTS,
                COUNT(DISTINCT PAYMENT_TYPE)     AS NUM_METHODS,
                MAX(PAYMENT_TYPE) KEEP (DENSE_RANK FIRST ORDER BY
                    SUM(TO_NUMBER(PAYMENT_VALUE)) DESC)
                    OVER (PARTITION BY ORDER_ID) AS PRIMARY_PAYMENT_TYPE
            FROM STG.STG_ORDER_PAYMENTS
            WHERE ETL_BATCH_ID = p_batch_id
            GROUP BY ORDER_ID
        ) p ON p.ORDER_ID = o.ORDER_ID
        -- Review (take highest score per order if multiple reviews)
        LEFT JOIN (
            SELECT ORDER_ID,
                   MAX(REVIEW_SCORE) AS REVIEW_SCORE,
                   MAX(REVIEW_ID)    AS REVIEW_ID
            FROM STG.STG_ORDER_REVIEWS
            WHERE ETL_BATCH_ID = p_batch_id
            GROUP BY ORDER_ID
        ) r ON r.ORDER_ID = o.ORDER_ID
        WHERE o.ETL_BATCH_ID = p_batch_id
          -- Idempotency: skip already loaded orders
          AND NOT EXISTS (
              SELECT 1 FROM DWH.FACT_ORDERS f
              WHERE f.ORDER_ID = o.ORDER_ID
          );

        v_cnt := SQL%ROWCOUNT;
        COMMIT;
        ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'SUCCESS',v_cnt,v_cnt);
        ETL_CTRL.PKG_ETL_LOGGER.set_watermark('OLIST_CSV','ORDERS',SYSDATE,p_batch_id);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'FAILED',p_err=>SQLERRM);
            RAISE;
    END load_fact_orders;

    -- ================================================================
    -- FACT_ORDER_ITEMS — one row per line item
    -- ================================================================
    PROCEDURE load_fact_order_items(p_batch_id IN NUMBER) IS
        v_step_id NUMBER;
        v_cnt     NUMBER;
    BEGIN
        v_step_id := ETL_CTRL.PKG_ETL_LOGGER.start_step(p_batch_id, 'LOAD_FACT_ORDER_ITEMS');

        INSERT INTO DWH.FACT_ORDER_ITEMS (
            ORDER_ID, ORDER_ITEM_ID, ORDER_DATE_KEY,
            CUSTOMER_KEY, PRODUCT_KEY, SELLER_KEY,
            PRICE, FREIGHT_VALUE,
            ETL_BATCH_ID, ETL_INSERT_DT
        )
        SELECT
            i.ORDER_ID,
            i.ORDER_ITEM_ID,
            to_date_key(o.ORDER_PURCHASE_TIMESTAMP),
            c.CUSTOMER_KEY,
            p.PRODUCT_KEY,
            s.SELLER_KEY,
            TO_NUMBER(i.PRICE),
            TO_NUMBER(i.FREIGHT_VALUE),
            p_batch_id,
            SYSDATE
        FROM STG.STG_ORDER_ITEMS     i
        JOIN STG.STG_ORDERS          o ON o.ORDER_ID    = i.ORDER_ID
                                      AND o.ETL_BATCH_ID = p_batch_id
        JOIN DWH.DIM_CUSTOMER        c ON c.CUSTOMER_ID = o.CUSTOMER_ID
        JOIN DWH.DIM_PRODUCT         p ON p.PRODUCT_ID  = i.PRODUCT_ID
                                      AND p.IS_CURRENT   = 'Y'
        JOIN DWH.DIM_SELLER          s ON s.SELLER_ID   = i.SELLER_ID
        WHERE i.ETL_BATCH_ID = p_batch_id
          AND NOT EXISTS (
              SELECT 1 FROM DWH.FACT_ORDER_ITEMS f
              WHERE f.ORDER_ID = i.ORDER_ID AND f.ORDER_ITEM_ID = i.ORDER_ITEM_ID
          );

        v_cnt := SQL%ROWCOUNT;
        COMMIT;
        ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'SUCCESS',v_cnt,v_cnt);
        ETL_CTRL.PKG_ETL_LOGGER.set_watermark('OLIST_CSV','ORDER_ITEMS',SYSDATE,p_batch_id);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'FAILED',p_err=>SQLERRM);
            RAISE;
    END load_fact_order_items;

    -- ================================================================
    -- FACT_REVIEWS
    -- ================================================================
    PROCEDURE load_fact_reviews(p_batch_id IN NUMBER) IS
        v_step_id NUMBER;
        v_cnt     NUMBER;
    BEGIN
        v_step_id := ETL_CTRL.PKG_ETL_LOGGER.start_step(p_batch_id, 'LOAD_FACT_REVIEWS');

        INSERT INTO DWH.FACT_REVIEWS (
            REVIEW_ID, ORDER_ID, REVIEW_DATE_KEY,
            REVIEW_SCORE, SENTIMENT,
            HAS_COMMENT_TITLE, HAS_COMMENT_MESSAGE, RESPONSE_DAYS,
            ETL_BATCH_ID, ETL_INSERT_DT
        )
        SELECT
            r.REVIEW_ID,
            r.ORDER_ID,
            to_date_key(r.REVIEW_CREATION_DATE),
            r.REVIEW_SCORE,
            CASE
                WHEN r.REVIEW_SCORE >= 4 THEN 'POSITIVE'
                WHEN r.REVIEW_SCORE = 3  THEN 'NEUTRAL'
                ELSE 'NEGATIVE'
            END,
            CASE WHEN r.REVIEW_COMMENT_TITLE    IS NOT NULL THEN 'Y' ELSE 'N' END,
            CASE WHEN r.REVIEW_COMMENT_MESSAGE  IS NOT NULL THEN 'Y' ELSE 'N' END,
            CASE WHEN r.REVIEW_ANSWER_TIMESTAMP IS NOT NULL
                      AND r.REVIEW_CREATION_DATE IS NOT NULL
                 THEN ROUND(
                     TO_DATE(SUBSTR(r.REVIEW_ANSWER_TIMESTAMP,1,10),'YYYY-MM-DD')
                     - TO_DATE(SUBSTR(r.REVIEW_CREATION_DATE,1,10),'YYYY-MM-DD')
                 )
            END,
            p_batch_id,
            SYSDATE
        FROM STG.STG_ORDER_REVIEWS r
        WHERE r.ETL_BATCH_ID = p_batch_id
          AND r.REVIEW_SCORE BETWEEN 1 AND 5   -- Skip DQ-rejected rows
          AND NOT EXISTS (
              SELECT 1 FROM DWH.FACT_REVIEWS f WHERE f.REVIEW_ID = r.REVIEW_ID
          );

        v_cnt := SQL%ROWCOUNT;
        COMMIT;
        ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'SUCCESS',v_cnt,v_cnt);
        ETL_CTRL.PKG_ETL_LOGGER.set_watermark('OLIST_CSV','ORDER_REVIEWS',SYSDATE,p_batch_id);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'FAILED',p_err=>SQLERRM);
            RAISE;
    END load_fact_reviews;

    -- ================================================================
    -- AGG_DAILY_SALES — pre-aggregated BI table
    -- ================================================================
    PROCEDURE refresh_agg_daily_sales(p_batch_id IN NUMBER) IS
        v_step_id NUMBER;
        v_cnt     NUMBER;
    BEGIN
        v_step_id := ETL_CTRL.PKG_ETL_LOGGER.start_step(p_batch_id, 'REFRESH_AGG_DAILY_SALES');

        -- Full refresh of aggregate (truncate & reload for simplicity)
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DWH.AGG_DAILY_SALES';

        INSERT INTO DWH.AGG_DAILY_SALES (
            DATE_KEY, STATE, CATEGORY_EN, PAYMENT_TYPE, ORDER_STATUS,
            ORDER_COUNT, ITEM_COUNT, TOTAL_REVENUE, TOTAL_FREIGHT,
            AVG_REVIEW_SCORE, LATE_DELIVERY_COUNT,
            ETL_BATCH_ID, ETL_INSERT_DT
        )
        SELECT
            fo.ORDER_DATE_KEY,
            dc.STATE,
            dp.CATEGORY_NAME_EN,
            fo.PAYMENT_TYPE_PRIMARY,
            fo.ORDER_STATUS,
            COUNT(DISTINCT fo.ORDER_ID)         AS ORDER_COUNT,
            COUNT(fi.ITEM_FACT_KEY)              AS ITEM_COUNT,
            ROUND(SUM(fi.PRICE),2)              AS TOTAL_REVENUE,
            ROUND(SUM(fi.FREIGHT_VALUE),2)      AS TOTAL_FREIGHT,
            ROUND(AVG(fo.REVIEW_SCORE),2)       AS AVG_REVIEW_SCORE,
            SUM(CASE WHEN fo.IS_LATE_DELIVERY='Y' THEN 1 ELSE 0 END) AS LATE_DELIVERY_COUNT,
            p_batch_id,
            SYSDATE
        FROM DWH.FACT_ORDERS      fo
        JOIN DWH.DIM_CUSTOMER     dc ON dc.CUSTOMER_KEY = fo.CUSTOMER_KEY
        JOIN DWH.FACT_ORDER_ITEMS fi ON fi.ORDER_ID     = fo.ORDER_ID
        JOIN DWH.DIM_PRODUCT      dp ON dp.PRODUCT_KEY  = fi.PRODUCT_KEY
                                     AND dp.IS_CURRENT   = 'Y'
        WHERE fo.ORDER_DATE_KEY IS NOT NULL
        GROUP BY
            fo.ORDER_DATE_KEY, dc.STATE, dp.CATEGORY_NAME_EN,
            fo.PAYMENT_TYPE_PRIMARY, fo.ORDER_STATUS;

        v_cnt := SQL%ROWCOUNT;
        COMMIT;
        ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'SUCCESS',v_cnt,v_cnt);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            ETL_CTRL.PKG_ETL_LOGGER.end_step(v_step_id,'FAILED',p_err=>SQLERRM);
            RAISE;
    END refresh_agg_daily_sales;

    -- ================================================================
    -- RUN_ALL — orchestrates all transforms in correct order
    -- ================================================================
    PROCEDURE run_all(p_batch_id IN NUMBER) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('--- Starting Transform Pipeline ---');
        validate_staging(p_batch_id);       -- 1. DQ First
        load_dim_customer(p_batch_id);      -- 2. Dimensions before facts
        load_dim_seller(p_batch_id);
        load_dim_product(p_batch_id);
        load_fact_orders(p_batch_id);       -- 3. Facts after dimensions
        load_fact_order_items(p_batch_id);
        load_fact_reviews(p_batch_id);
        refresh_agg_daily_sales(p_batch_id); -- 4. Aggregates last
        DBMS_OUTPUT.PUT_LINE('--- Transform Pipeline Complete ---');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('FATAL in run_all: ' || SQLERRM);
            RAISE;
    END run_all;

END PKG_TRANSFORM;
/
