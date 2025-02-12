#!/bin/bash

# Target DB credentials
DB_TARGET_DB=stock_reports
DB_TARGET_HOST=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_host'))" 2>/dev/null)
DB_TARGET_PORT=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_port'))" 2>/dev/null)
DB_TARGET_USER=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_user'))" 2>/dev/null)
DB_TARGET_PASSWORD=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_password'))" 2>/dev/null)


# Function to execute SQL commands
execute_sql() {
    mysql -h "${DB_TARGET_HOST}" -P "${DB_TARGET_PORT}" -u "${DB_TARGET_USER}" -p"${DB_TARGET_PASSWORD}" "${DB_TARGET_DB}" -e "$1"
}

# SQL command to truncate the report_merged_non_bundle table
SQL_TRUNCATE_COMMAND="TRUNCATE TABLE report_merged_non_bundle;"

# SQL commands to insert data from each source table into report_merged_non_bundle
SQL_INSERT_COMMANDS=(
    "INSERT INTO report_merged_non_bundle (order_id, created_at, quantity, warehouse_sku) SELECT order_id, created_at, quantity, warehouse_sku FROM report_abc_non_bundle;"
    "INSERT INTO report_merged_non_bundle (order_id, created_at, quantity, warehouse_sku) SELECT order_id, created_at, quantity, warehouse_sku FROM report_def_non_bundle;"
    "INSERT INTO report_merged_non_bundle (order_id, created_at, quantity, warehouse_sku) SELECT order_id, created_at, quantity, warehouse_sku FROM report_ghi_non_bundle;"
    "INSERT INTO report_merged_non_bundle (order_id, created_at, quantity, warehouse_sku) SELECT order_id, created_at, quantity, warehouse_sku FROM report_jkl_non_bundle;"
    "INSERT INTO report_merged_non_bundle (order_id, created_at, quantity, warehouse_sku) SELECT order_id, created_at, quantity, warehouse_sku FROM report_mno_non_bundle;"
)

# Execute the TRUNCATE command
echo "Truncating report_merged_non_bundle table..."
execute_sql "$SQL_TRUNCATE_COMMAND"

# Execute the INSERT commands
for cmd in "${SQL_INSERT_COMMANDS[@]}"; do
    echo "Inserting data into report_merged_non_bundle..."
    execute_sql "$cmd"
done

# Log message to confirm the execution
echo "Data insertion from all source tables has been completed successfully."
