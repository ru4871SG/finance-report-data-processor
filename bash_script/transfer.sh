#!/bin/bash

# List of brands
BRANDS=("ABC" "DEF" "GHI" "JKL" "MNO")

# List of tables to export
CONFIG_EXPORT_TABLES=(
    "processing_first_item_cost"
    "processing_second_item_cost"
    "product_cost"
    "sylius_product_variant"
    "sylius_product"
    "sylius_order_tasks"
    "sylius_order_item"
    "sylius_order_action_history"
    "sylius_admin_user"
    "sylius_customer"
    "sylius_promotion_coupon"
    "sylius_payment"
    "sylius_rbac_administration_role"
    "sylius_product_translation"
    "sylius_channel_pricing"
    "sylius_channel_pricing_item"
)

# Function to get database name for a brand
get_db_name() {
    local brand=$1
    echo "crm_${brand,,}"  # ${brand,,} converts to lowercase
}

# Function to get export location for a brand
get_export_location() {
    local brand=$1
    echo "/tmp/export-${brand,,}"
}

# Function to get log file name for a brand
get_log_file() {
    local brand=$1
    echo "/tmp/table_creation_log_${brand,,}.txt"
}

# Function to join array elements
join() {
    local IFS="$1"
    shift
    echo "$*"
}

# Function to process a specific brand
process_brand() {
    local brand=$1
    local db_name=$(get_db_name "$brand")
    local export_location=$(get_export_location "$brand")
    local log_file=$(get_log_file "$brand")
    
    echo "Processing brand: $brand (Database: $db_name)"

    # SOURCE DB
    # Retrieve the variables from Airflow, but suppress any output to the logs
    CONFIG_DB_HOST=$(python3 -c "from airflow.models import Variable; print(Variable.get('source_crm_db_host'))" 2>/dev/null)
    CONFIG_DB_NAME=$(python3 -c "from airflow.models import Variable; print(Variable.get('source_crm_db_name_${brand,,}'))" 2>/dev/null)
    CONFIG_DB_USER=$(python3 -c "from airflow.models import Variable; print(Variable.get('source_crm_db_user'))" 2>/dev/null)
    CONFIG_DB_PASSWORD=$(python3 -c "from airflow.models import Variable; print(Variable.get('source_crm_db_password'))" 2>/dev/null)

    # Target DB credentials
    DB_TARGET_DB=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_name_${brand,,}'))" 2>/dev/null)
    DB_TARGET_HOST=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_host'))" 2>/dev/null)
    DB_TARGET_PORT=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_port'))" 2>/dev/null)
    DB_TARGET_USER=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_user'))" 2>/dev/null)
    DB_TARGET_PASSWORD=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_password'))" 2>/dev/null)

    # Clean and create export directory
    rm -rf "${export_location}"
    mkdir "${export_location}"

    # Process each table
    for t in "${CONFIG_EXPORT_TABLES[@]}"; do
        i=0
        COLUMNS=()

        # Get all columns
        while IFS=$'\t' read column_name; do
            COLUMNS[$i]=$column_name
            ((i++))
        done < <(mysql --user="${CONFIG_DB_USER}" --skip-column-names --password="${CONFIG_DB_PASSWORD}" --database="${CONFIG_DB_NAME}" --host="${CONFIG_DB_HOST}" --execute="select column_name from information_schema.columns where table_schema = '${CONFIG_DB_NAME}' and table_name='$t' order by ORDINAL_POSITION")

        COLUMNS_STRING=$(join , "${COLUMNS[@]}")

        # Create mysqldump command to export the table
        (
            filename="${export_location}/${t}_tmp.sql"
            mysqldump --user="${CONFIG_DB_USER}" --password="${CONFIG_DB_PASSWORD}" --host="${CONFIG_DB_HOST}" \
                --skip-comments --skip-set-charset --single-transaction --disable-keys --no-tablespaces \
                -w "0=1 union select ${COLUMNS_STRING} from $t" "${CONFIG_DB_NAME}" "$t" | 
            sed "s/DROP TABLE IF EXISTS \`$t\`/DROP TABLE IF EXISTS \`${t}_tmp\`/g" | \
            sed "s/CREATE TABLE \`$t\`/CREATE TABLE \`${t}_tmp\`/g" |
            sed "s/INSERT INTO \`$t\`/INSERT INTO \`${t}_tmp\`/g" |
            sed 's/ json DEFAULT / LONGTEXT DEFAULT /g' | 
            sed '/^\/\*!/d' | 
            sed 's/ENGINE=InnoDB //g' | 
            sed '/CONSTRAINT.*FOREIGN KEY/d' | 
            sed 's/ AUTO_INCREMENT=[0-9]*//g' | 
            sed 's/DEFAULT CHARSET=[a-zA-Z0-9_]* COLLATE=[a-zA-Z0-9_]*//g' | 
            sed ':a;N;$!ba;s/,\n\s*)/)/g' | 
            sed 's/DEFAULT NULL//g' | 
            sed '/LOCK TABLES .* WRITE;/d; /UNLOCK TABLES;/d' | 
            sed 's/CHARACTER SET [a-zA-Z0-9_]\+//g' | 
            sed 's/COLLATE [a-zA-Z0-9_]\+//g' | 
            sed 's/CHECK (json_valid(`configuration`))//g' > "$filename"
            echo "Table exported for $brand: ${t}"
        ) &
    done

    wait

    # Initialize log file
    echo "transfer_crm_${brand,,} log for $(date)" > "$log_file"

    # Import tables
    for file in ${export_location}/*.sql; do 
        table_name=$(basename "$file" .sql)
        echo "Attempting to create table: $table_name for $brand"
        mysql -h ${DB_TARGET_HOST} -P ${DB_TARGET_PORT} -u ${DB_TARGET_USER} -p${DB_TARGET_PASSWORD} ${DB_TARGET_DB} < $file 2>/dev/null

        # Check if the table exists
        if mysql -h ${DB_TARGET_HOST} -P ${DB_TARGET_PORT} -u ${DB_TARGET_USER} -p${DB_TARGET_PASSWORD} -e "DESCRIBE \`${DB_TARGET_DB}\`.\`${table_name}\`;" 2>/dev/null; then
            echo "Successfully created table: $table_name for $brand" | tee -a "$log_file"
        else
            echo "Failed to create table: $table_name for $brand" | tee -a "$log_file"
            return 1
        fi
    done

    echo "Finished processing $brand"
}

# Individual brand functions
process_brand_abc() {
    process_brand "ABC"
}

process_brand_def() {
    process_brand "DEF"
}

process_brand_ghi() {
    process_brand "GHI"
}

process_brand_jkl() {
    process_brand "JKL"
}

process_brand_mno() {
    process_brand "MNO"
}

# Function to validate brand name
validate_brand() {
    local brand=$1
    for valid_brand in "${BRANDS[@]}"; do
        if [[ "${valid_brand}" == "${brand}" ]]; then
            return 0
        fi
    done
    return 1
}

# Function to process any brand
process_brand_by_name() {
    local brand=$1
    if ! validate_brand "$brand"; then
        echo "Error: Invalid brand '${brand}'. Must be one of: ${BRANDS[*]}"
        exit 1
    fi
    process_brand "$brand"
}

# Main execution - Check for command line argument
if [[ $# -eq 1 ]]; then
    # If a brand name is provided, process just that brand
    process_brand_by_name "$1"
else
    # Default behavior (for testing) - process ABC
    process_brand_abc
fi
