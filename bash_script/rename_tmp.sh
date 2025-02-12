#!/bin/bash

# List of brands
BRANDS=("ABC" "DEF" "GHI" "JKL" "MNO")

# Function to get database name for a brand
get_db_name() {
    local brand=$1
    echo "crm_${brand,,}"  # ${brand,,} converts to lowercase
}

# List of tables to process
TABLES=(
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

# Function to process a specific brand
process_brand() {
    local brand=$1
    local db_name=$(get_db_name "$brand")
    
    echo "Processing brand: $brand (Database: $db_name)"

    # Target DB credentials
    DB_TARGET_DB=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_name_${brand,,}'))" 2>/dev/null)
    DB_TARGET_HOST=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_host'))" 2>/dev/null)
    DB_TARGET_PORT=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_port'))" 2>/dev/null)
    DB_TARGET_USER=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_user'))" 2>/dev/null)
    DB_TARGET_PASSWORD=$(python3 -c "from airflow.models import Variable; print(Variable.get('target_db_password'))" 2>/dev/null)

    # Function to execute SQL commands
    execute_sql() {
        mysql -h "${DB_TARGET_HOST}" -P "${DB_TARGET_PORT}" -u "${DB_TARGET_USER}" -p"${DB_TARGET_PASSWORD}" "${DB_TARGET_DB}" -e "$1"
    }

    # Function to check if a table has data
    table_has_data() {
        local table_name=$1
        local row_count=$(execute_sql "SELECT COUNT(*) FROM \`${table_name}\`;" | tail -n 1)
        [ "$row_count" -gt 0 ]
    }

    # Check if all temporary tables have data
    all_tmp_tables_have_data() {
        for table in "${TABLES[@]}"; do
            if ! table_has_data "${table}_tmp"; then
                echo "ERROR: Temporary table ${table}_tmp does not exist or is empty for $brand."
                return 1
            fi
        done
        return 0
    }

    # Main process for this brand
    if all_tmp_tables_have_data; then
        echo "All temporary tables exist and have data. Proceeding with renaming process for $brand."
        
        for table in "${TABLES[@]}"; do
            echo "Processing table: $table for $brand"
            
            # Drop the main table
            execute_sql "DROP TABLE IF EXISTS \`$table\`;"
            echo "Dropped main table: $table"
            
            # Rename the temporary table to become the new main table
            execute_sql "RENAME TABLE \`${table}_tmp\` TO \`$table\`;"
            echo "Renamed ${table}_tmp to $table"
            
            echo "Completed processing table: $table for $brand"
        done
        
        echo "Rename operation completed successfully for all tables in $brand."
    else
        echo "ERROR: Not all temporary tables exist or have data for $brand. Aborting the renaming process."
        echo "No tables were renamed or dropped. Please check the temporary tables and try again."
        return 1
    fi
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
