#!/bin/bash
# =============================================================================
# PROJECT   : Olist Brazilian E-Commerce ETL Data Pipeline
# FILE      : scripts/run_sqlldr.sh
# PURPOSE   : Load all 9 Olist CSV files using SQL*Loader, then run ETL pipeline
# USAGE     : chmod +x scripts/run_sqlldr.sh && ./scripts/run_sqlldr.sh
# =============================================================================

set -e  # Exit on error

# ---- CONFIGURATION — Update these for your environment ----
DB_USER="STG"
DB_PASS="StgPass123#"
DB_HOST="localhost"
DB_PORT="1521"
DB_SVC="ORCL"
CONNECT_STR="${DB_USER}/${DB_PASS}@//${DB_HOST}:${DB_PORT}/${DB_SVC}"

ETL_USER="ETL_CTRL"
ETL_PASS="EtlPass123#"
ETL_CONNECT="${ETL_USER}/${ETL_PASS}@//${DB_HOST}:${DB_PORT}/${DB_SVC}"

CSV_DIR="/data/olist/csv"
LOG_DIR="/data/olist/logs"
CTL_DIR="./sqlldr"

# Create log directory if missing
mkdir -p "${LOG_DIR}"

echo "============================================="
echo "  Olist ETL Pipeline — SQL*Loader Phase"
echo "  Started: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================="

# ---- FUNCTION: Run SQL*Loader ----
run_loader() {
    local CTL_FILE=$1
    local LOG_FILE=$2
    local DESC=$3

    echo ""
    echo ">>> Loading: ${DESC}"
    sqlldr userid="${CONNECT_STR}" \
            control="${CTL_FILE}" \
            log="${LOG_FILE}" \
            bad="${LOG_FILE%.log}.bad" \
            direct=TRUE \
            rows=50000 \
            errors=1000

    if [ $? -eq 0 ]; then
        echo "    SUCCESS: ${DESC}"
    else
        echo "    WARNING: ${DESC} completed with errors — check ${LOG_FILE}"
    fi
}

# ---- Step 1: Truncate Staging Tables ----
echo ""
echo ">>> Step 1: Truncating staging tables..."
sqlplus -s "${CONNECT_STR}" <<EOF
SET SERVEROUTPUT ON
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG.STG_ORDERS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG.STG_ORDER_ITEMS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG.STG_ORDER_PAYMENTS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG.STG_ORDER_REVIEWS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG.STG_CUSTOMERS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG.STG_SELLERS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG.STG_PRODUCTS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG.STG_GEOLOCATION';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG.STG_CATEGORY_TRANSLATION';
    DBMS_OUTPUT.PUT_LINE('All staging tables truncated.');
END;
/
EXIT
EOF

# ---- Step 2: Load All CSV Files ----
echo ""
echo ">>> Step 2: Loading CSV files via SQL*Loader..."

run_loader "${CTL_DIR}/09_category_translation.ctl"  "${LOG_DIR}/09_category.log"   "Category Translation (load first — needed for product mapping)"
run_loader "${CTL_DIR}/05_customers.ctl"             "${LOG_DIR}/05_customers.log"  "Customers"
run_loader "${CTL_DIR}/06_sellers.ctl"               "${LOG_DIR}/06_sellers.log"    "Sellers"
run_loader "${CTL_DIR}/07_products.ctl"              "${LOG_DIR}/07_products.log"   "Products"
run_loader "${CTL_DIR}/08_geolocation.ctl"           "${LOG_DIR}/08_geolocation.log" "Geolocation (~1M rows)"
run_loader "${CTL_DIR}/01_orders.ctl"                "${LOG_DIR}/01_orders.log"     "Orders"
run_loader "${CTL_DIR}/02_order_items.ctl"           "${LOG_DIR}/02_order_items.log" "Order Items"
run_loader "${CTL_DIR}/03_order_payments.ctl"        "${LOG_DIR}/03_payments.log"   "Order Payments"
run_loader "${CTL_DIR}/04_order_reviews.ctl"         "${LOG_DIR}/04_reviews.log"    "Order Reviews"

echo ""
echo ">>> Step 2 Complete: All CSV files loaded to staging."

# ---- Step 3: Run PL/SQL ETL Pipeline ----
echo ""
echo ">>> Step 3: Running PL/SQL Transform & Load pipeline..."
sqlplus -s "${ETL_CONNECT}" <<EOF
SET SERVEROUTPUT ON SIZE UNLIMITED
BEGIN
    ETL_CTRL.PKG_ETL_MASTER.run_pipeline('SQLLDR_SCRIPT');
END;
/
EXIT
EOF

echo ""
echo "============================================="
echo "  ETL Pipeline Complete!"
echo "  Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================="

# ---- Step 4: Health Check ----
echo ""
echo ">>> Step 4: Health Check..."
sqlplus -s "${ETL_CONNECT}" <<EOF
SET SERVEROUTPUT ON
BEGIN
    ETL_CTRL.PKG_ETL_MASTER.health_check;
END;
/
EXIT
EOF
