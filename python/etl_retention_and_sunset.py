"""
Script to process retention_table and sunset_table
"""

import mysql.connector
import pandas as pd

# Load environment variables from Airflow
from airflow.models import Variable

# List of all brands
BRANDS = ['ABC', 'DEF', 'GHI', 'JKL', 'MNO']

def get_target_db_details(brand):
    """Get target database details for the given brand"""
    return {
        'database': Variable.get(f'target_db_name_{brand.lower()}'),
        'user': Variable.get('target_db_user'),
        'password': Variable.get('target_db_password'),
        'host': Variable.get('target_db_host'),
        'port': Variable.get('target_db_port')
    }

def extract():
    """Extract required data"""
    try:
        connection = mysql.connector.connect(
            database=source_crm_db_name,
            user=target_db_user,
            password=target_db_password,
            host=target_db_host,
            port=target_db_port
        )
        print("Connected to MySQL successfully for retention data extraction")
        cursor = connection.cursor(dictionary=True)

        # Query for orders
        orders_query = """
            SELECT 
                so.id, so.customer_id, so.created_at, so.state, so.is_subscription,
                so.created_from_order_id,
                sc.email
            FROM sylius_order so
            LEFT JOIN sylius_customer sc ON so.customer_id = sc.id
            WHERE so.state IN ('fulfilled', 'new')
            AND so.payment_state IN ('paid', 'partially_refunded', 'refunded')
            AND so.total > 0
        """
        cursor.execute(orders_query)
        orders_results = cursor.fetchall()
        orders_df = pd.DataFrame(orders_results)

        # Query for order items
        items_query = """
            SELECT 
                soi.order_id, soi.id, 
                COALESCE(NULLIF(soi.product_name, ''), spt.name) as product_name,
                soi.variant_name, soi.quantity,
                sp.id as product_id
            FROM sylius_order_item soi
            JOIN sylius_product_variant spv ON soi.variant_id = spv.id
            JOIN sylius_product sp ON spv.product_id = sp.id
            LEFT JOIN sylius_product_translation spt ON sp.id = spt.translatable_id
        """
        cursor.execute(items_query)
        items_results = cursor.fetchall()
        items_df = pd.DataFrame(items_results)

        cursor.close()
        connection.close()

        return {
            'orders': orders_df,
            'order_items': items_df
        }

    except mysql.connector.Error as error:
        print(f"Error while connecting to MySQL: {error}")
        return {
            'orders': pd.DataFrame(),
            'order_items': pd.DataFrame()
        }

def optimize_check_same_product(retention_df, items_df, first_items):
    """Vectorized implementation of checking for same product purchases"""
    # Create a mapping of first items
    first_items_map = first_items[['order_id', 'product_name', 'id']].copy()
    
    # Merge with items to find duplicates
    merged = items_df.merge(
        first_items_map, 
        on=['order_id', 'product_name'], 
        suffixes=('', '_first')
    )
    
    # Find orders with same product
    orders_with_same = merged[
        merged['id'] != merged['id_first']
    ]['order_id'].unique()
    
    return retention_df['first_order_id'].isin(orders_with_same)

def get_second_orders_bulk(orders_df, emails):
    """Get second orders efficiently for all customers"""
    # Filter orders for relevant customers
    customer_orders = orders_df[
        (orders_df['email'].isin(emails)) & 
        (orders_df['state'].isin(['fulfilled', 'new']))
    ].copy()
    
    # Sort orders for each customer
    customer_orders.sort_values(['email', 'created_at', 'id'], inplace=True)
    
    # Get second order for each customer
    second_orders = customer_orders.groupby('email').nth(1)
    
    return second_orders

def process_retention_table(dfs):
    """Process data for retention_table"""
    orders_df = dfs['orders'].copy()
    items_df = dfs['order_items'].copy()
    
    # Convert id columns to int and create indices
    orders_df['id'] = orders_df['id'].astype(int)
    orders_df['customer_id'] = orders_df['customer_id'].astype(int)
    items_df['order_id'] = items_df['order_id'].astype(int)
    items_df['id'] = items_df['id'].astype(int)
    
    print(f"After initial orders query: {len(orders_df)}")
    
    # Filter for valid orders
    valid_orders = orders_df[orders_df['state'].isin(['fulfilled', 'new'])]

    # Get orders that actually have items - filter out order_id with no items (from sylius_order_item)
    orders_with_items = valid_orders[valid_orders['id'].isin(items_df['order_id'].unique())]
    
    # Get the first order (with items) for each email (customer)
    first_orders = orders_with_items.groupby('email').agg({
        'id': 'min',
        'created_at': 'min',
        'customer_id': 'first'
    }).reset_index()
    first_orders.columns = ['email', 'first_order_id', 'first_order_date', 'customer_id']
    first_orders['first_order_id'] = first_orders['first_order_id'].astype(int)
    first_orders['customer_id'] = first_orders['customer_id'].astype(int)
    
    print(f"\nAfter first_orders: {len(first_orders)}")
    
    # Get total order counts per email
    order_counts = valid_orders.groupby('email').size().reset_index(name='order_count')
    
    # Mark each row in items_df with a row number per order_id
    items_df['rn'] = items_df.groupby('order_id')['id'].rank(method='first')
    items_df['total_items_count'] = items_df.groupby('order_id')['id'].transform('count')
    
    # First and second item per order
    first_items = items_df[items_df['rn'] == 1].copy()
    second_items = items_df[items_df['rn'] == 2].copy()
    second_items = second_items.rename(columns={'product_name': 'first_order_second_product_name'})[['order_id', 'first_order_second_product_name']]
    
    # Build the retention_df starting with first_orders
    retention_df = (
        first_orders
        .merge(order_counts, on='email')
        .merge(
            orders_df[['id', 'is_subscription']], 
            left_on='first_order_id', 
            right_on='id',
            how='left'
        )
        .rename(columns={'is_subscription': 'first_order_subscription'})
    )
    
    print(f"\nAfter subscription merge: {len(retention_df)}")
    
    # Merge in details for the first item
    retention_df = retention_df.merge(
        first_items[['order_id', 'product_name', 'variant_name', 'quantity', 'total_items_count']],
        left_on='first_order_id',
        right_on='order_id',
        how='left'
    ).rename(columns={
        'product_name': 'first_product_name',
        'variant_name': 'first_product_variant',
        'quantity': 'first_product_quantity',
        'total_items_count': 'first_order_total_item_count'
    })

    print(f"\nAfter items merge: {len(retention_df)}")

    # Add logging for orders without items
    empty_first_orders = retention_df[
        (retention_df['first_product_name'].isna()) | 
        (retention_df['first_product_name'] == '')
    ]
    
    if not empty_first_orders.empty:
        print("\nWARNING: The following customer_ids have first orders with no items:")
        customer_ids = empty_first_orders['customer_id'].tolist()
        order_ids = empty_first_orders['first_order_id'].tolist()
        for cust_id, order_id in zip(customer_ids, order_ids):
            print(f"customer_id {cust_id} (first_order_id: {order_id})")
        print(f"Total count of such cases: {len(empty_first_orders)}\n")
    else:
        print("\nThere's no customer_id without empty first order, fortunately.\n")

    
    # Fill any missing values
    retention_df['first_product_name'] = retention_df['first_product_name'].fillna('')
    retention_df['first_product_variant'] = retention_df['first_product_variant'].fillna('')
    retention_df['first_product_quantity'] = retention_df['first_product_quantity'].fillna(0)
    retention_df['first_order_total_item_count'] = retention_df['first_order_total_item_count'].fillna(0)
    
    # Calculate upsell flags
    retention_df['bought_upsell_more_of_the_same'] = optimize_check_same_product(
        retention_df[retention_df['first_order_total_item_count'] > 1],
        items_df,
        first_items
    )
    retention_df['bought_upsell_more_of_the_same'] = retention_df['bought_upsell_more_of_the_same'].fillna(False)
    retention_df['bought_any_upsell'] = retention_df['first_order_total_item_count'] > 1
    
    # Merge in the second item from the first order
    retention_df = retention_df.merge(
        second_items,
        left_on='first_order_id',
        right_on='order_id',
        how='left'
    )
    
    retention_df['first_order_second_product_name'] = retention_df['first_order_second_product_name'].fillna('')
    
    # Get second orders for each email
    second_orders = get_second_orders_bulk(orders_df, retention_df['email'])
    # 'second_orders' is a DF with the second order as a row, but 'id' is the second order's ID
    # Keep only the columns we need (email + 'id') and rename
    second_orders = second_orders[['email', 'id']].rename(columns={'id': 'second_order_id'})
    # If email is in the index, reset it
    second_orders = second_orders.reset_index(drop=True)
    
    # Merge second_order_id into retention_df
    retention_df = retention_df.merge(
        second_orders,
        on='email',
        how='left'
    )
    
    # Get the "first item" of that second order using items_df
    second_order_items = items_df[items_df['rn'] == 1][['order_id', 'product_name']]
    second_order_items = second_order_items.rename(columns={'product_name': 'first_item_from_second_order'})
    
    # Merge so each row in retention_df has the product name from the second order's first item
    retention_df = retention_df.merge(
        second_order_items,
        left_on='second_order_id',
        right_on='order_id',
        how='left'
    )
    retention_df['first_item_from_second_order'] = retention_df['first_item_from_second_order'].fillna('')
    
    # Decide final 'second_item_product_name'
    retention_df['second_item_product_name'] = retention_df.apply(
        lambda row: (
            row['first_order_second_product_name']
            if row['first_order_second_product_name'] != ''
            else row['first_item_from_second_order']
        ),
        axis=1
    )
    
    # Convert columns to final datatypes
    numeric_columns = ['order_count', 'first_order_total_item_count', 'first_product_quantity']
    for col in numeric_columns:
        retention_df[col] = retention_df[col].fillna(0).astype(int)

    string_columns = ['first_product_name', 'first_product_variant', 'second_item_product_name']
    for col in string_columns:
        retention_df[col] = retention_df[col].fillna('')

    boolean_columns = ['first_order_subscription', 'bought_upsell_more_of_the_same', 'bought_any_upsell']
    for col in boolean_columns:
        retention_df[col] = retention_df[col].fillna(False)
    
    # Final check for any remaining NaNs
    if retention_df.isna().any().any():
        print(
            "Warning: NaN values still present in columns:", 
            retention_df.columns[retention_df.isna().any()].tolist()
        )
    
    # Return only the needed columns
    return retention_df[[
        'email', 
        'customer_id', 
        'first_order_date', 
        'order_count', 
        'first_order_id', 
        'first_product_name', 
        'first_product_variant', 
        'first_product_quantity', 
        'first_order_total_item_count', 
        'first_order_subscription', 
        'bought_upsell_more_of_the_same', 
        'bought_any_upsell', 
        'second_item_product_name'
    ]]



def process_sunset_table(dfs):
    """Process data for sunset_table"""
    orders_df = dfs['orders']
    items_df = dfs['order_items']
    
    # Filter non-subscription orders and no created_from_order_id
    non_sub_orders = orders_df[
        (~orders_df['is_subscription']) & 
        (orders_df['created_from_order_id'].isna()) &
        (orders_df['state'].isin(['fulfilled', 'new']))
    ]
    
    # Get first orders
    first_orders = non_sub_orders.groupby('email').agg({
        'id': 'min',
        'created_at': 'min',
        'customer_id': 'first'
    }).reset_index()
    first_orders.columns = ['email', 'first_order_id', 'first_order_date', 'customer_id']
    
    # Count total orders per customer
    order_counts = orders_df.groupby('email').size().reset_index(name='order_count')
    
    # Get second orders
    def get_second_order(group):
        ordered_group = group.sort_values('id')
        if len(ordered_group) < 2:
            return None
        return ordered_group.iloc[1]
    
    second_orders = non_sub_orders.groupby('customer_id').apply(
        get_second_order
    ).reset_index(drop=True)
    
    # Merge first and second orders
    sunset_df = first_orders.merge(order_counts)
    sunset_df = sunset_df.merge(
        second_orders[['customer_id', 'id', 'created_at', 'is_subscription']], 
        on='customer_id', 
        suffixes=('', '_second')
    )
    
    # Calculate days between orders
    sunset_df['days_between_first_and_second_order'] = (
        pd.to_datetime(sunset_df['created_at']) - 
        pd.to_datetime(sunset_df['first_order_date'])
    ).dt.days
    
    # Get order items for both orders
    first_items = items_df.merge(
        sunset_df[['first_order_id']], 
        left_on='order_id', 
        right_on='first_order_id'
    )
    second_items = items_df.merge(
        sunset_df[['id']], 
        left_on='order_id', 
        right_on='id'
    )
    
    # Get first items from both orders
    first_order_first_item = first_items.groupby('order_id').first().reset_index()
    second_order_first_item = second_items.groupby('order_id').first().reset_index()
    second_order_first_item = second_order_first_item.rename(
        columns={'product_name': 'second_order_first_product_name'}
    )
    
    # Calculate total items in first order
    first_order_totals = first_items.groupby('order_id').size().reset_index(name='first_order_total_item_count')
    
    # Calculate more_of_the_same flag
    def has_same_product(order_id, product_name):
        order_items = first_items[first_items['order_id'] == order_id]
        return (order_items['product_name'] == product_name).sum() > 1
    
    # Merge all information
    sunset_df = sunset_df.merge(
        first_order_first_item[['order_id', 'product_name', 'variant_name', 'quantity']], 
        left_on='first_order_id', 
        right_on='order_id'
    )
    sunset_df = sunset_df.merge(
        second_order_first_item[['order_id', 'second_order_first_product_name']], 
        left_on='id', 
        right_on='order_id',
        suffixes=('', '_second')
    )
    sunset_df = sunset_df.merge(
        first_order_totals, 
        left_on='first_order_id', 
        right_on='order_id'
    )
    
    # Calculate bought_upsell_more_of_the_same
    sunset_df['bought_upsell_more_of_the_same'] = sunset_df.apply(
        lambda x: has_same_product(x['first_order_id'], x['product_name']) if x['first_order_total_item_count'] > 1 else False,
        axis=1
    )
    
    # Rename columns to match schema
    sunset_df = sunset_df.rename(columns={
        'created_at': 'second_order_date',
        'id': 'second_order_id',
        'product_name': 'first_product_name',
        'variant_name': 'first_product_variant',
        'quantity': 'first_product_quantity',
        'is_subscription': 'first_order_subscription'
    })
    
    return sunset_df[['email', 'customer_id', 'first_order_date', 'second_order_date', 
                     'days_between_first_and_second_order', 'first_order_id', 
                     'second_order_id', 'order_count', 'first_product_name',
                     'first_product_variant', 'first_product_quantity', 
                     'first_order_total_item_count', 'first_order_subscription',
                     'second_order_first_product_name', 'bought_upsell_more_of_the_same']]


def load_table(df, table_name, connection, chunk_size=7000):
    """Load function to insert data into MySQL table"""
    if df.empty:
        print("No new rows to insert.")
        return

    try:
        cursor = connection.cursor()
        connection.autocommit = False

        # Truncate table before inserting new data
        print(f"Truncating table '{table_name}'...")
        cursor.execute(f"TRUNCATE TABLE {table_name}")

        columns = df.columns
        columns_list_str = ', '.join(f"`{col}`" for col in columns)
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({columns_list_str}) VALUES ({placeholders})"

        total_inserted = 0
        total_chunks = (len(df) + chunk_size - 1) // chunk_size
        
        # Insert data in chunks with executemany
        for chunk_num, start in enumerate(range(0, len(df), chunk_size), 1):
            end = start + chunk_size
            chunk = df.iloc[start:end]
            values = [tuple(row) for row in chunk.values]
            
            cursor.executemany(insert_query, values)
            connection.commit()
            
            total_inserted += len(chunk)
            print(f"Processed chunk {chunk_num}/{total_chunks} - Inserted {total_inserted}/{len(df)} rows...")

        connection.autocommit = True
        print(f"Successfully inserted all {total_inserted} rows.")
        cursor.close()

    except mysql.connector.Error as error:
        print(f"Error while loading data to MySQL: {error}")
        connection.rollback()
        raise

def etl_process(brand):
    """ETL process for the given brand"""
    try:
        print(f"Starting ETL process for {brand}")
        db_details = get_target_db_details(brand)

        global source_crm_db_name, target_db_user, target_db_password, target_db_host, target_db_port
        source_crm_db_name = db_details['database']
        target_db_user = db_details['user']
        target_db_password = db_details['password']
        target_db_host = db_details['host']
        target_db_port = db_details['port']

        connection = mysql.connector.connect(
            database=source_crm_db_name,
            user=target_db_user,
            password=target_db_password,
            host=target_db_host,
            port=target_db_port
        )

        print("Extracting data...")
        dfs = extract()

        # Process retention_table
        print("Processing retention table...")
        retention_df = process_retention_table(dfs)

        # Process sunset_table
        print("Processing sunset table...")
        sunset_df = process_sunset_table(dfs)

        print("Loading data...")
        load_table(retention_df, 'retention_table', connection)
        load_table(sunset_df, 'sunset_table', connection)
        
        connection.close()
        print(f"ETL process completed for {brand}")
        
    except Exception as e:
        print(f"An error occurred during the ETL process for {brand}: {str(e)}")
        if 'connection' in locals():
            connection.close()
        raise

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
