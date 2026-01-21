# Importancion de Librerias a Usar
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}


def clean_sales_data(df):
    """
    Clean and transform raw sales data.
    
    Args:
        df (pd.DataFrame): Raw dataframe from CSV
        
    Returns:
        pd.DataFrame: Cleaned dataframe ready for insertion
    """
    print("🔧 Starting data cleaning...")
    initial_rows = len(df)
    
    required_columns = [
        'ORDERNUMBER', 'ORDERLINENUMBER', 'ORDERDATE',
        'STATUS', 'PRODUCTCODE', 'PRODUCTLINE',
        'CUSTOMERNAME', 'COUNTRY', 'QUANTITYORDERED',
        'PRICEEACH', 'SALES', 'DEALSIZE'
    ]
    
    df = df[required_columns].copy()
    print(f"   ✓ Selected {len(required_columns)} columns")
    
    column_mapping = {
        'ORDERNUMBER': 'order_number',
        'ORDERLINENUMBER': 'order_line_number',
        'ORDERDATE': 'order_date',
        'STATUS': 'status',
        'PRODUCTCODE': 'product_code',
        'PRODUCTLINE': 'product_line',
        'CUSTOMERNAME': 'customer_name',
        'COUNTRY': 'country',
        'QUANTITYORDERED': 'quantity',
        'PRICEEACH': 'unit_price',
        'SALES': 'total_amount',
        'DEALSIZE': 'deal_size'
    }
    
    df.rename(columns=column_mapping, inplace=True)
    print(f"   ✓ Renamed columns to snake_case")
    
    def parse_date(date_str):
        """Parse multiple date formats."""
        if pd.isna(date_str):
            return None
        
        # Intentar diferentes formatos
        date_formats = [
            '%m/%d/%Y %H:%M',  # 2/24/2003 0:00
            '%m/%d/%Y',         # 2/24/2003
            '%Y-%m-%d',         # 2003-02-24
        ]
        
        for fmt in date_formats:
            try:
                return pd.to_datetime(date_str, format=fmt).date()
            except:
                continue
        
        # Si ningún formato funciona, intentar parseo automático
        try:
            return pd.to_datetime(date_str).date()
        except:
            return None
    
    df['order_date'] = df['order_date'].apply(parse_date)
    
    # Eliminar filas con fechas inválidas
    date_nulls = df['order_date'].isna().sum()
    if date_nulls > 0:
        print(f"   ⚠ Removed {date_nulls} rows with invalid dates")
        df = df[df['order_date'].notna()]
    
    print(f"   ✓ Parsed dates successfully")

    # Rellenar nulos en campos opcionales
    df['status'] = df['status'].fillna('Unknown')
    df['product_line'] = df['product_line'].fillna('Uncategorized')
    df['customer_name'] = df['customer_name'].fillna('Unknown Customer')
    df['country'] = df['country'].fillna('Unknown')
    df['deal_size'] = df['deal_size'].fillna('Unknown')
    
    # Eliminar filas con nulos en campos críticos
    critical_columns = ['order_number', 'order_line_number', 'product_code', 
                       'quantity', 'unit_price', 'total_amount']
    
    nulls_before = len(df)
    df = df.dropna(subset=critical_columns)
    nulls_removed = nulls_before - len(df)
    
    if nulls_removed > 0:
        print(f"   ⚠ Removed {nulls_removed} rows with nulls in critical fields")
    
    print(f"   ✓ Handled missing values")
    
    # Convertir tipos numéricos
    df['order_number'] = pd.to_numeric(df['order_number'], errors='coerce').astype('Int64')
    df['order_line_number'] = pd.to_numeric(df['order_line_number'], errors='coerce').astype('Int64')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('Int64')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
    
    # Eliminar filas con conversiones fallidas
    df = df.dropna(subset=['order_number', 'order_line_number', 'quantity', 
                           'unit_price', 'total_amount'])
    
    # Validar que valores numéricos sean positivos
    df = df[df['quantity'] > 0]
    df = df[df['unit_price'] >= 0]
    df = df[df['total_amount'] >= 0]
    
    print(f"   ✓ Converted and validated data types")
    
    duplicates_before = len(df)
    df = df.drop_duplicates(subset=['order_number', 'order_line_number'], keep='first')
    duplicates_removed = duplicates_before - len(df)
    
    if duplicates_removed > 0:
        print(f"   ⚠ Removed {duplicates_removed} duplicate rows")
    
    print(f"   ✓ Removed duplicates")
    
    final_rows = len(df)
    rows_removed = initial_rows - final_rows
    
    print(f"\n📊 Cleaning Summary:")
    print(f"   Initial rows: {initial_rows}")
    print(f"   Final rows: {final_rows}")
    print(f"   Rows removed: {rows_removed} ({rows_removed/initial_rows*100:.1f}%)")
    
    return df

# ====================================================================
# DATABASE CONNECTION AND INSERTION
# ====================================================================

def connect_to_database():
    """
    Establish connection to PostgreSQL database.
    
    Returns:
        psycopg2.connection: Database connection object
    """
    try:
        print("🔌 Connecting to PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("   ✓ Connected successfully")
        return conn
    except psycopg2.Error as e:
        print(f"   ✗ Connection failed: {e}")
        sys.exit(1)


def insert_data(conn, df):
    """
    Insert cleaned data into raw_sales table using batch insertion.
    
    Args:
        conn: PostgreSQL connection object
        df (pd.DataFrame): Cleaned dataframe
    """
    print(f"\n📥 Inserting {len(df)} rows into database...")
    
    cursor = conn.cursor()
    
    # Prepare INSERT query
    insert_query = """
        INSERT INTO raw_sales (
            order_number, order_line_number, order_date, status,
            product_code, product_line, customer_name, country,
            quantity, unit_price, total_amount, deal_size
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (order_number, order_line_number) 
        DO NOTHING;
    """
    
    # Convert dataframe to list of tuples
    data_tuples = [
        (
            row['order_number'],
            row['order_line_number'],
            row['order_date'],
            row['status'],
            row['product_code'],
            row['product_line'],
            row['customer_name'],
            row['country'],
            row['quantity'],
            row['unit_price'],
            row['total_amount'],
            row['deal_size']
        )
        for _, row in df.iterrows()
    ]
    
    try:
        # Use execute_batch for efficient insertion
        execute_batch(cursor, insert_query, data_tuples, page_size=100)
        conn.commit()
        
        # Get actual inserted count
        cursor.execute("SELECT COUNT(*) FROM raw_sales;")
        total_rows = cursor.fetchone()[0]
        
        print(f"   ✓ Insertion completed successfully")
        print(f"   ✓ Total rows in table: {total_rows}")
        
    except psycopg2.Error as e:
        conn.rollback()
        print(f"   ✗ Insertion failed: {e}")
        raise
    finally:
        cursor.close()

def get_table_stats(conn):
    """
    Retrieve and display table statistics.
    
    Args:
        conn: PostgreSQL connection object
    """
    print(f"\n📊 Database Statistics:")
    
    cursor = conn.cursor()
    
    # Query 1: Total records
    cursor.execute("SELECT COUNT(*) FROM raw_sales;")
    total_records = cursor.fetchone()[0]
    print(f"   Total records: {total_records}")
    
    # Query 2: Date range
    cursor.execute("""
        SELECT 
            MIN(order_date) as earliest_date,
            MAX(order_date) as latest_date
        FROM raw_sales;
    """)
    date_range = cursor.fetchone()
    print(f"   Date range: {date_range[0]} to {date_range[1]}")
    
    # Query 3: Total sales amount
    cursor.execute("SELECT SUM(total_amount) FROM raw_sales;")
    total_sales = cursor.fetchone()[0]
    print(f"   Total sales: ${total_sales:,.2f}")
    
    # Query 4: Unique customers
    cursor.execute("SELECT COUNT(DISTINCT customer_name) FROM raw_sales;")
    unique_customers = cursor.fetchone()[0]
    print(f"   Unique customers: {unique_customers}")
    
    # Query 5: Product lines
    cursor.execute("SELECT COUNT(DISTINCT product_line) FROM raw_sales;")
    product_lines = cursor.fetchone()[0]
    print(f"   Product lines: {product_lines}")
    
    cursor.close()

