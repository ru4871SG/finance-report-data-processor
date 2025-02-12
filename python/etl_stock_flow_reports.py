"""
Script to process Stock Flow reports
"""

import mysql.connector
import pandas as pd

# Load environment variables from Airflow
from airflow.models import Variable

# List of all brands
BRANDS = ['ABC', 'DEF', 'GHI', 'JKL', 'MNO']

# def get_source_db_details(brand):
#     return {
#         'database': Variable.get(f'source_crm_db_name_{brand.lower()}'),
#         'user': Variable.get('source_crm_db_user'),
#         'password': Variable.get('source_crm_db_password'),
#         'host': Variable.get('source_crm_db_host'),
#         'port': Variable.get('source_crm_db_port')
#     }

# Here we use the variables for target DB. We will transfer the tables from source DB to target DB 
#  using Bash Script, before we run this Python script in the Airflow DAG
def get_source_db_details(brand):
    """Get source database details for the given brand"""
    return {
        'database': Variable.get(f'target_db_name_{brand.lower()}'),
        'user': Variable.get('target_db_user'),
        'password': Variable.get('target_db_password'),
        'host': Variable.get('target_db_host'),
        'port': Variable.get('target_db_port')
    }

def get_target_db_details():
    """Get target database details for the given brand"""
    return {
        'database': Variable.get('target_db_name_stock_reports'),
        'user': Variable.get('target_db_user'),
        'password': Variable.get('target_db_password'),
        'host': Variable.get('target_db_host'),
        'port': Variable.get('target_db_port')
    }

def get_target_table_names(brand):
    """Function to get target table names for each brand"""
    brand_lower = brand.lower()
    return {
        'non_bundle': f'report_{brand_lower}_non_bundle',
        'only_bundle': f'report_{brand_lower}_only_bundle'
    }

def extract():
    """Extract required data"""
    try:
        connection = mysql.connector.connect(
            database=source_crm_db_name,
            user=source_crm_db_user,
            password=source_crm_db_password,
            host=source_crm_db_host,
            port=source_crm_db_port
        )
        print("Connected to MySQL (Source DB) successfully.")

        cursor = connection.cursor(dictionary=True)

        # SQL query
        query = f"""
        SELECT DISTINCT
            soi.order_id,
            so.created_at,
            so.updated_at,
            so.payment_state,
            soi.quantity,
            soi.unit_price,
            soi.units_total,
            spv.product_id,
            soi.variant_id,
            soi.product_name,
            soi.variant_name,
            scp.promotion_warehouse_sku AS scp_promotion_warehouse_sku,
            scpi.promotion_warehouse_sku AS scpi_promotion_warehouse_sku,
            scpi.count,
            sp.mint_soft_sku
        FROM
            sylius_order_item soi
        LEFT JOIN
            sylius_product_variant spv ON soi.variant_id = spv.id
        LEFT JOIN
            sylius_product sp ON sp.id = spv.product_id
        LEFT JOIN
            sylius_channel_pricing scp ON spv.id = scp.product_variant_id
        LEFT JOIN
            sylius_channel_pricing_item scpi ON scp.id = scpi.channel_pricing_id
        LEFT JOIN
            sylius_order so ON soi.order_id = so.id
        WHERE
            so.payment_state IN('paid', 'partially_paid', 'partially_refunded', 'refunded')
            AND mint_soft_sku IS NOT NULL
        ORDER BY soi.order_id
        """

        cursor.execute(query)
        results = cursor.fetchall()

        df = pd.DataFrame(results)

        cursor.close()
        connection.close()

        return df

    except mysql.connector.Error as error:
        print(f"Error while connecting to MySQL: {error}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


def transform(df):
    """Main transform function to process the extracted data"""
    # Create separate DataFrames for each table
    sylius_channel_pricing = df[['order_id', 'created_at', 'updated_at', 'quantity', 'unit_price', 'units_total', 'scp_promotion_warehouse_sku', 'scpi_promotion_warehouse_sku', 'mint_soft_sku', 'product_id', 'variant_id', 'product_name', 'variant_name', 'payment_state']]
    sylius_channel_pricing_item = df[['order_id', 'count', 'scpi_promotion_warehouse_sku']]

    # Function to apply the logic for warehouse encoding
    def create_reporting_df(order_item_df, pricing_item_df):
        reporting_df = order_item_df.copy()

        # Add original_quantity column
        reporting_df['original_quantity'] = reporting_df['quantity']

        # Group pricing items by order_id
        pricing_groups = pricing_item_df.groupby('order_id')
        
        def get_sku_and_count(row, pricing_group):
            if pricing_group.empty or (pd.isna(row['scp_promotion_warehouse_sku']) and pricing_group['scpi_promotion_warehouse_sku'].isna().all()):
                return pd.Series({'final_sku': row['mint_soft_sku'], 'applied_count': 0, 'quantity': row['quantity'], 'count': 0})
            
            # Sort pricing items by count in descending order
            sorted_pricing = pricing_group.sort_values('count', ascending=False)
            
            for _, price_item in sorted_pricing.iterrows():
                if row['quantity'] >= price_item['count'] and not pd.isna(price_item['scpi_promotion_warehouse_sku']):
                    final_sku = price_item['scpi_promotion_warehouse_sku']
                    applied_count = price_item['count']
                    break
            else:
                final_sku = row['scp_promotion_warehouse_sku'] if not pd.isna(row['scp_promotion_warehouse_sku']) else row['mint_soft_sku']
                applied_count = 0
            
            # Handle the sku and quantity logic
            sku_parts = final_sku.split('#')
            final_sku = sku_parts[0]
            
            if len(sku_parts) > 1:
                try:
                    sku_quantity = int(sku_parts[1])
                    if sku_quantity != 0 and not pd.isna(row['scpi_promotion_warehouse_sku']) and row['scpi_promotion_warehouse_sku'] != '':
                        quantity = sku_quantity
                    else:
                        quantity = row['quantity']
                except ValueError:
                    quantity = row['quantity']
            else:
                quantity = row['quantity']
            
            return pd.Series({'final_sku': final_sku, 'applied_count': applied_count, 'quantity': quantity, 'count': applied_count})
        
        # Apply the get_sku_and_count function to each row, passing the corresponding pricing group
        reporting_df[['final_sku', 'applied_count', 'quantity', 'count']] = reporting_df.apply(
            lambda row: get_sku_and_count(row, pricing_groups.get_group(row['order_id']) if row['order_id'] in pricing_groups.groups else pd.DataFrame()),
            axis=1
        )
        
        reporting_df['row_num'] = range(len(reporting_df))
        return reporting_df

    reporting_df = create_reporting_df(sylius_channel_pricing, sylius_channel_pricing_item)
    column_order = ['row_num', 'order_id', 'created_at', 'updated_at', 'quantity', 'original_quantity', 'unit_price', 'units_total', 'scp_promotion_warehouse_sku', 'scpi_promotion_warehouse_sku', 'final_sku', 'applied_count', 'count', 'mint_soft_sku', 'product_id', 'variant_id', 'product_name', 'variant_name', 'payment_state']
    reporting_df = reporting_df[column_order]

    # Function to drop duplicates based on scpi_promotion_warehouse_sku presence
    def drop_duplicates_based_on_sku(df):
        df_sorted = df.sort_values(by='scpi_promotion_warehouse_sku', ascending=False)  # Ensure rows with scpi_sku come first
        return df_sorted.drop_duplicates(subset=['order_id', 'unit_price', 'units_total', 'product_id', 'variant_id', 'product_name', 'variant_name', 'final_sku'], keep='first')

    reporting_df = drop_duplicates_based_on_sku(reporting_df)
    reporting_df = reporting_df.sort_values(by='order_id', ascending=True)
    reporting_df['row_num'] = range(len(reporting_df))
    return reporting_df


def duplicate_rows_with_pipe(df):
    """Function to specifically handle the rows with pipe |"""
    def split_row(row, prev_row):
        new_rows = []
        bundle_sku = None

        for col in ['scpi_promotion_warehouse_sku', 'scp_promotion_warehouse_sku']:
            if pd.notna(row[col]) and '|' in str(row[col]):
                skus = str(row[col]).split('|')
                # we store the original bundle SKU in bundle_sku (e.g., "ABC#4|DEF#0"), including if the number is #0
                bundle_sku = str(row[col])
                for sku in skus:
                    new_row = row.copy()
                    sku_parts = sku.split('#')
                    new_row['final_sku'] = sku_parts[0]
                    new_row['bundle_sku'] = bundle_sku
                    if len(sku_parts) > 1:
                        try:
                            sku_quantity = int(sku_parts[1])
                            if sku_quantity != 0:
                                new_row['quantity'] = sku_quantity
                            else:
                                new_row['quantity'] = row['quantity']
                        except ValueError:
                            new_row['quantity'] = row['quantity']
                    else:
                        new_row['quantity'] = row['quantity']
                    new_rows.append(new_row)
                return new_rows
        
        # Handle case where both scpi and scp are None
        if pd.isna(row['scpi_promotion_warehouse_sku']) and pd.isna(row['scp_promotion_warehouse_sku']):
            if (prev_row is not None and 
                prev_row['order_id'] == row['order_id'] and 
                row['row_num'] - prev_row['row_num'] == 1 and
                (pd.notna(prev_row['scpi_promotion_warehouse_sku']) or pd.notna(prev_row['scp_promotion_warehouse_sku']))):
                new_row = row.copy()
                new_row['final_sku'] = row['mint_soft_sku']
                new_row['bundle_sku'] = None 
                new_rows.append(new_row)
                return new_rows
        
        # If no special case applies, return the original row
        row['bundle_sku'] = None
        return [row]

    new_rows = []
    prev_row = None
    for _, row in df.iterrows():
        new_rows.extend(split_row(row, prev_row))
        prev_row = row

    return pd.DataFrame(new_rows)


def check_bundle_etc(df):
    """Function to separate bundle and non-bundle rows"""
    # Check which order_ids have a pipe character "|" in scpi_promotion_warehouse_sku or scp_promotion_warehouse_sku
    bundle_orders = df.groupby('order_id').apply(
        lambda x: x['scpi_promotion_warehouse_sku'].str.contains(r'\|', na=False).any() or 
                  x['scp_promotion_warehouse_sku'].str.contains(r'\|', na=False).any()
    )
    
    # Map this information back to the original dataframe
    df['is_bundle'] = df['order_id'].map(bundle_orders)

    # re-create row_num to match the new index
    df['row_num'] = range(len(df))

    # change 'final_sku' column name to 'warehouse_sku'
    df.rename(columns={'final_sku': 'warehouse_sku'}, inplace=True)
    
    return df


def preparing_non_bundle(df):
    """Function to prepare non-bundle data"""
    # Create a new dataframe with only non-bundle rows and reset the index
    non_bundle_df = df[df['is_bundle'] == False].copy()
    non_bundle_df.reset_index(drop=True, inplace=True)

    # Create a new dataframe with only bundle rows and reset the index
    only_bundle_df = df[df['is_bundle'] == True].copy()
    only_bundle_df.reset_index(drop=True, inplace=True)

    # Define sorting key - each order_id with bundle has multiple rows, so we need to prioritize the one with bundles
    def sorting_key(row):
        if pd.notna(row['scp_promotion_warehouse_sku']) and pd.notna(row['scpi_promotion_warehouse_sku']):
            return 0
        elif pd.notna(row['scpi_promotion_warehouse_sku']):
            return 1
        elif pd.notna(row['scp_promotion_warehouse_sku']):
            return 2
        else:
            return 3

    # Sort the dataframe for only_bundle_df
    only_bundle_df['sort_key'] = only_bundle_df.apply(sorting_key, axis=1)
    only_bundle_df = only_bundle_df.sort_values(['order_id', 'sort_key'])

    # Drop the temporary sorting column for only_bundle_df
    only_bundle_df = only_bundle_df.drop(columns=['sort_key'])

    # Combine both dataframes back to non_bundle_df
    non_bundle_df = pd.concat([non_bundle_df, only_bundle_df], ignore_index=True)
    
    # Sort the combined dataframe by order_id if needed
    non_bundle_df = non_bundle_df.sort_values('order_id').reset_index(drop=True)
    
    # Reset row_num to match the new index
    non_bundle_df['row_num'] = non_bundle_df.index

    print(f"Created non-bundle and only-bundle dataframe with {len(non_bundle_df)} rows.")     
    return non_bundle_df


def preparing_bundle(df):
    """Function to prepare only-bundle data"""
    # Create a new dataframe with only bundle rows and reset the index
    only_bundle_df = df[df['is_bundle'] == True].copy()
    only_bundle_df.reset_index(drop=True, inplace=True)

    # Clean up bundle_sku by removing #number patterns
    def clean_bundle_sku(sku):
        if pd.isna(sku):
            return sku
        # Split by pipe, clean each part, then rejoin
        parts = str(sku).split('|')
        cleaned_parts = [part.split('#')[0] for part in parts]
        return '|'.join(cleaned_parts)
    
    only_bundle_df['bundle_sku'] = only_bundle_df['bundle_sku'].apply(clean_bundle_sku)
    
    # Define sorting key - each order_id with bundle has multiple rows, so we need to prioritize the one with bundles
    def sorting_key(row):
        if pd.notna(row['scp_promotion_warehouse_sku']) and pd.notna(row['scpi_promotion_warehouse_sku']):
            return 0
        elif pd.notna(row['scpi_promotion_warehouse_sku']):
            return 1
        elif pd.notna(row['scp_promotion_warehouse_sku']):
            return 2
        else:
            return 3

    # Sort the dataframe
    only_bundle_df['sort_key'] = only_bundle_df.apply(sorting_key, axis=1)
    only_bundle_df = only_bundle_df.sort_values(['order_id', 'sort_key'])

    # Create new columns
    only_bundle_df['bundle_product_name'] = ''
    only_bundle_df['bundle_variant_name'] = ''
    only_bundle_df['bundle_product_id'] = None
    only_bundle_df['bundle_variant_id'] = None
    only_bundle_df['bundle_quantity'] = None

    # Update new columns based on the first row of each order_id
    for order_id in only_bundle_df['order_id'].unique():
        order_group = only_bundle_df[only_bundle_df['order_id'] == order_id]
        first_row = order_group.iloc[0]
        
        only_bundle_df.loc[only_bundle_df['order_id'] == order_id, 'bundle_product_name'] = first_row['product_name']
        only_bundle_df.loc[only_bundle_df['order_id'] == order_id, 'bundle_variant_name'] = first_row['variant_name']
        only_bundle_df.loc[only_bundle_df['order_id'] == order_id, 'bundle_product_id'] = first_row['product_id']
        only_bundle_df.loc[only_bundle_df['order_id'] == order_id, 'bundle_variant_id'] = first_row['variant_id']
        only_bundle_df.loc[only_bundle_df['order_id'] == order_id, 'bundle_quantity'] = first_row['original_quantity']

    # Drop the temporary sorting column
    only_bundle_df = only_bundle_df.drop(columns=['sort_key'])

    # Rename old columns
    only_bundle_df.rename(columns={'product_id': 'old_product_id', 'variant_id': 'old_variant_id', 'product_name': 'old_product_name', 'variant_name': 'old_variant_name'}, inplace=True)

    # Keep only the first row for each order_id, since we have sorted out the rows that show the bundle first for each order_id
    only_bundle_df = only_bundle_df.drop_duplicates(subset=['order_id'], keep='first')

    print(f"Created only-bundle dataframe with {len(only_bundle_df)} rows.")
    return only_bundle_df


def load_non_bundle(df, target_table='report_apex_non_bundle', chunk_size=7000):
    """Load data to the target table (non-bundle data)"""
    # Select only the specified columns
    columns_to_load = ['order_id', 'created_at', 'quantity', 'warehouse_sku']
    df_to_load = df[columns_to_load]

    try:
        # Establish a connection using mysql.connector
        connection = mysql.connector.connect(
            database=target_db_name,
            user=target_db_user,
            password=target_db_password,
            host=target_db_host,
            port=target_db_port
        )
        
        # Set autocommit to False
        connection.autocommit = False
        
        cursor = connection.cursor()

        # Truncate the target table
        truncate_query = f"TRUNCATE TABLE {target_table}"
        cursor.execute(truncate_query)
        print(f"Table '{target_table}' has been truncated.")

        # Prepare the insert query
        insert_query = f"""
        INSERT INTO {target_table} (order_id, created_at, quantity, warehouse_sku)
        VALUES (%s, %s, %s, %s)
        """

        # Prepare all data for insertion
        data_to_insert = [
            (int(row['order_id']), row['created_at'], int(row['quantity']), str(row['warehouse_sku']))
            for _, row in df_to_load.iterrows()
        ]

        # Insert data in chunks with executemany
        total_inserted = 0
        for i in range(0, len(data_to_insert), chunk_size):
            chunk = data_to_insert[i:i + chunk_size]
            cursor.executemany(insert_query, chunk)
            total_inserted += len(chunk)
            print(f"Inserted {total_inserted} rows so far...")

        # Commit the changes
        connection.commit()

        # Set autocommit back to True
        connection.autocommit = True

        print(f"Inserted {total_inserted} new row(s) in total.")

        cursor.close()
        connection.close()

    except mysql.connector.Error as error:
        print(f"Error while connecting to the MySQL database: {error}")


def load_only_bundle(df, target_table='report_apex_only_bundle', chunk_size=7000):
    """Load data to the target table (only-bundle data)"""
    # Select only the specified columns
    columns_to_load = ['order_id', 'created_at', 'bundle_product_id', 'bundle_product_name', 'bundle_variant_id', 'bundle_variant_name', 'bundle_quantity', 'bundle_sku']
    df_to_load = df[columns_to_load]

    try:
        # Establish a connection using mysql.connector
        connection = mysql.connector.connect(
            database=target_db_name,
            user=target_db_user,
            password=target_db_password,
            host=target_db_host,
            port=target_db_port
        )
        
        # Set autocommit to False
        connection.autocommit = False
        
        cursor = connection.cursor()

        # Truncate the target table
        truncate_query = f"TRUNCATE TABLE {target_table}"
        cursor.execute(truncate_query)
        print(f"Table '{target_table}' has been truncated.")

        # Prepare the insert query
        insert_query = f"""
        INSERT INTO {target_table} (order_id, created_at, bundle_product_id, bundle_product_name, bundle_variant_id, bundle_variant_name, bundle_quantity, bundle_sku)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Prepare all data for insertion
        data_to_insert = [
            (int(row['order_id']), row['created_at'], int(row['bundle_product_id']), str(row['bundle_product_name']),
             int(row['bundle_variant_id']), str(row['bundle_variant_name']), int(row['bundle_quantity']), row['bundle_sku'] if pd.notna(row['bundle_sku']) else None)
            for _, row in df_to_load.iterrows()
        ]

        # Insert data in chunks with executemany
        total_inserted = 0
        for i in range(0, len(data_to_insert), chunk_size):
            chunk = data_to_insert[i:i + chunk_size]
            cursor.executemany(insert_query, chunk)
            total_inserted += len(chunk)
            print(f"Inserted {total_inserted} rows so far...")

        # Commit the changes
        connection.commit()

        # Set autocommit back to True
        connection.autocommit = True

        print(f"Inserted {total_inserted} new row(s) in total.")

        cursor.close()
        connection.close()

    except mysql.connector.Error as error:
        print(f"Error while connecting to the MySQL database: {error}")

    print("Data loading completed successfully.")


def etl_process(brand):
    """ETL process for the given brand"""
    try:
        print(f"Starting ETL process for {brand}")

        # Set up database connections using brand-specific details
        global source_crm_db_name, source_crm_db_user, source_crm_db_password, source_crm_db_host, source_crm_db_port
        global target_db_name, target_db_user, target_db_password, target_db_host, target_db_port

        source_details = get_source_db_details(brand)
        target_details = get_target_db_details()

        # Set source connection details
        source_crm_db_name = source_details['database']
        source_crm_db_user = source_details['user']
        source_crm_db_password = source_details['password']
        source_crm_db_host = source_details['host']
        source_crm_db_port = source_details['port']

        # Set target connection details
        target_db_name = target_details['database']
        target_db_user = target_details['user']
        target_db_password = target_details['password']
        target_db_host = target_details['host']
        target_db_port = target_details['port']

        print("Extracting data...")
        extracted_data = extract()

        if extracted_data.empty:
            print("No data to process.")
            return

        print("Transforming data...")
        global reporting_df
        reporting_df = transform(extracted_data)

        print("Duplicating rows with pipe...")
        global reporting_pipe_df
        reporting_pipe_df = duplicate_rows_with_pipe(reporting_df)

        print("Adding bundles filter...")
        global reporting_pipe_df_add_ons_bundle
        reporting_pipe_df_add_ons_bundle = check_bundle_etc(reporting_pipe_df)

        print("Preparing non-bundle data...")
        global non_bundle_df
        non_bundle_df = preparing_non_bundle(reporting_pipe_df_add_ons_bundle)

        print("Preparing only-bundle data...")
        global only_bundle_df
        only_bundle_df = preparing_bundle(reporting_pipe_df_add_ons_bundle)

        # Get brand-specific table names
        tables = get_target_table_names(brand)

        print(f"Loading non-bundle data to {tables['non_bundle']}...")
        load_non_bundle(non_bundle_df, tables['non_bundle'])

        print(f"Loading only-bundle data to {tables['only_bundle']}...")
        load_only_bundle(only_bundle_df, tables['only_bundle'])

        print(f"ETL process completed successfully for {brand}!")
    except Exception as e:
        print(f"An error occurred during the ETL process for {brand}: {str(e)}")


def run_etl_process_by_brand(brand):
    """Run ETL process"""
    if brand not in BRANDS:
        raise ValueError(f"Invalid brand: {brand}. Must be one of {BRANDS}")
    etl_process(brand)


# Individual brand ETL functions
def run_etl_process_abc():
    etl_process('ABC')

def run_etl_process_def():
    etl_process('DEF')

def run_etl_process_ghi():
    etl_process('GHI')

def run_etl_process_jkl():
    etl_process('JKL')

def run_etl_process_mno():
    etl_process('MNO')

if __name__ == "__main__":
    for brand in BRANDS:
        run_etl_process_by_brand(brand)
