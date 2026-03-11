-- FILE: sqlldr/01_orders.ctl
-- SOURCE: olist_orders_dataset.csv
-- TARGET: STG.STG_ORDERS
-- COMMAND:
--   sqlldr userid=STG/pass@//host:1521/ORCL control=sqlldr/01_orders.ctl log=logs/01_orders.log bad=logs/01_orders.bad direct=TRUE rows=50000

LOAD DATA
INFILE '/data/olist/csv/olist_orders_dataset.csv'
BADFILE '/data/olist/logs/orders.bad'
DISCARDFILE '/data/olist/logs/orders.dsc'
APPEND
INTO TABLE STG.STG_ORDERS
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
    ORDER_ID                        CHAR(50),
    CUSTOMER_ID                     CHAR(50),
    ORDER_STATUS                    CHAR(30),
    ORDER_PURCHASE_TIMESTAMP        CHAR(30),
    ORDER_APPROVED_AT               CHAR(30),
    ORDER_DELIVERED_CARRIER_DATE    CHAR(30),
    ORDER_DELIVERED_CUSTOMER_DATE   CHAR(30),
    ORDER_ESTIMATED_DELIVERY_DATE   CHAR(30),
    ETL_BATCH_ID                    CONSTANT "0",
    ETL_INSERT_DT                   SYSDATE,
    ETL_SOURCE_FILE                 CONSTANT "olist_orders_dataset.csv"
)
