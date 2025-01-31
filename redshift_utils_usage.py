# Initialize the utility
redshift_utils = RedshiftDataUtils(
    region='us-west-2',
    secret_name='your/secret/name'
)

# Execute a simple query
df = redshift_utils.execute_query(
    "SELECT * FROM sales WHERE date >= '2024-01-01'",
    mode='pandas'
)

# Read specific columns with condition
df = redshift_utils.read_table(
    schema='public',
    table_name='sales',
    columns=['date', 'amount', 'category'],
    where_clause="category = 'Electronics'",
    mode='pandas'
)

# Read same data using PySpark
spark_df = redshift_utils.read_table(
    schema='public',
    table_name='sales',
    columns=['date', 'amount', 'category'],
    where_clause="category = 'Electronics'",
    mode='pyspark'
)

# Execute multiple queries in batch
queries = [
    "UPDATE sales SET processed = true WHERE date = '2024-01-01'",
    "DELETE FROM temp_sales WHERE date < '2024-01-01'",
    "INSERT INTO sales_summary SELECT date, SUM(amount) FROM sales GROUP BY date"
]

query_ids = redshift_utils.batch_execute_queries(queries)

# Combine multiple operations
def analyze_sales_data():
    # Get daily sales
    daily_sales = redshift_utils.execute_query("""
        SELECT 
            date,
            category,
            SUM(amount) as total_sales,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM sales
        WHERE date >= DATEADD(day, -30, CURRENT_DATE)
        GROUP BY date, category
    """)
    
    # Get top products
    top_products = redshift_utils.execute_query("""
        SELECT 
            product_id,
            product_name,
            SUM(amount) as revenue,
            COUNT(*) as units_sold
        FROM sales s
        JOIN products p ON s.product_id = p.id
        WHERE date >= DATEADD(day, -30, CURRENT_DATE)
        GROUP BY product_id, product_name
        ORDER BY revenue DESC
        LIMIT 10
    """)
    
    return daily_sales, top_products