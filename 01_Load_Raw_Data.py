# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Load Raw Data
# MAGIC
# MAGIC This notebook loads CSV files from the source directory into raw tables in the data lake.
# MAGIC
# MAGIC ## What this notebook does:
# MAGIC - Reads CSV files from source directory
# MAGIC - Loads data into raw tables with minimal transformation
# MAGIC - Adds metadata columns (load timestamp, source file)
# MAGIC - Validates data loading

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp, when
from pyspark.sql.types import *
import os
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Configuration from Previous Notebook

# COMMAND ----------

# Now safely get them
dq_agent_catalog_name = dbutils.widgets.get("dq_agent_catalog_name")
dq_agent_schema_name = dbutils.widgets.get("dq_agent_schema_name")
dq_agent_raw_schema_name = dbutils.widgets.get("dq_agent_raw_schema_name")
dq_agent_processed_schema_name = dbutils.widgets.get("dq_agent_processed_schema_name")
dq_agent_final_schema_name = dbutils.widgets.get("dq_agent_final_schema_name")
csv_folder_path = dbutils.widgets.get("csv_folder_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Table Schemas

# COMMAND ----------

# Define schemas for each table
table_schemas = {
    "orders": StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("promised_delivery_time", TimestampType(), True),
        StructField("actual_delivery_time", TimestampType(), True),
        StructField("delivery_status", StringType(), True),
        StructField("order_total", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("delivery_partner_id", StringType(), True),
        StructField("store_id", StringType(), True)
    ]),
    
    "customers": StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("area", StringType(), True),
        StructField("pincode", StringType(), True),
        StructField("registration_date", DateType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("total_orders", IntegerType(), True),
        StructField("avg_order_value", DoubleType(), True)
    ]),
    
    "delivery_performance": StructType([
        StructField("order_id", StringType(), True),
        StructField("delivery_partner_id", StringType(), True),
        StructField("promised_time", TimestampType(), True),
        StructField("actual_time", TimestampType(), True),
        StructField("delivery_time_minutes", DoubleType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("delivery_status", StringType(), True),
        StructField("reasons_if_delayed", StringType(), True)
    ]),
    
    "products": StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("brand", StringType(), True),
        StructField("description", StringType(), True),
        StructField("is_available", BooleanType(), True)
    ]),
    
    "order_items": StructType([
        StructField("order_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_price", DoubleType(), True)
    ]),
    
    "inventory": StructType([
        StructField("product_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("quantity_available", IntegerType(), True),
        StructField("reorder_level", IntegerType(), True),
        StructField("last_updated", TimestampType(), True)
    ]),
    
    "marketing_performance": StructType([
        StructField("campaign_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("budget", DoubleType(), True),
        StructField("spent", DoubleType(), True),
        StructField("impressions", IntegerType(), True),
        StructField("clicks", IntegerType(), True),
        StructField("conversions", IntegerType(), True)
    ]),
    
    "customer_feedback": StructType([
        StructField("feedback_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("feedback_text", StringType(), True),
        StructField("feedback_date", TimestampType(), True),
        StructField("category", StringType(), True)
    ])
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading Function

# COMMAND ----------

def load_csv_to_table(csv_file_name, table_name, schema):
    """
    Load CSV file into a table with the specified schema
    
    Args:
        csv_file_name (str): Name of the CSV file
        table_name (str): Target table name
        schema (StructType): Schema for the table
    """
    try:
        # Construct full file path
        file_path = f"{csv_folder_path}/{csv_file_name}"
        
        # Check if file exists
        #f not os.path.exists(file_path):
            #print(f"⚠️ File not found: {file_path}")
            #return False
            
        # Read CSV with schema
        df = spark.read.csv(
            file_path,
            header=True,
            schema=schema,
            inferSchema=True,
            timestampFormat="yyyy-MM-dd HH:mm:ss"
        )
        
        # Add metadata columns
        df_with_metadata = df.withColumn("_load_timestamp", current_timestamp()) \
                            .withColumn("_source_file", lit(csv_file_name)) \
                            .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
        
        # Write to table
        full_table_name = f"{dq_agent_catalog_name}.{dq_agent_raw_schema_name}.{table_name}"
        
        # Write in overwrite mode for full refresh
        df_with_metadata.write.mode("overwrite").saveAsTable(full_table_name)
        
        # Get row count
        row_count = df_with_metadata.count()
        
        print(f"✅ Loaded {row_count} rows into {full_table_name}")
        return True
        
    except Exception as e:
        print(f"❌ Error loading {csv_file_name} to {table_name}: {str(e)}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load All Tables

# COMMAND ----------

# Define file mappings
file_mappings = {
    "archive_blinkit_orders.csv": ("raw_blinkit_orders", table_schemas["orders"]),
    "archive_blinkit_customers.csv": ("raw_blinkit_customers", table_schemas["customers"]),
    "archive_blinkit_delivery_performance.csv": ("raw_blinkit_delivery_performance", table_schemas["delivery_performance"]),
    "archive_blinkit_products.csv": ("raw_blinkit_products", table_schemas["products"]),
    "archive_blinkit_order_items.csv": ("raw_blinkit_order_items", table_schemas["order_items"]),
    "archive_blinkit_inventory.csv": ("raw_blinkit_inventory", table_schemas["inventory"]),
    "archive_blinkit_marketing_performance.csv": ("raw_blinkit_marketing_performance", table_schemas["marketing_performance"]),
    "archive_blinkit_customer_feedback.csv": ("raw_blinkit_customer_feedback", table_schemas["customer_feedback"])
}

# COMMAND ----------

print("🚀 Starting data loading process...")
print("=" * 50)

success_count = 0
total_count = len(file_mappings)

for csv_file, (table_name, schema) in file_mappings.items():
    print(f"\n📥 Loading {csv_file}...")
    if load_csv_to_table(csv_file, table_name, schema):
        success_count += 1

print("\n" + "=" * 50)
print(f"✅ Data loading complete! {success_count}/{total_count} tables loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data Loading

# COMMAND ----------

# List all tables in raw schema
print("📋 Tables in raw schema:")
tables = spark.sql(f"SHOW TABLES IN {dq_agent_catalog_name}.{dq_agent_raw_schema_name}").collect()
for table in tables:
    table_name = table.tableName
    row_count = spark.sql(f"SELECT COUNT(*) as count FROM {dq_agent_catalog_name}.{dq_agent_raw_schema_name}.{table_name}").collect()[0]["count"]
    print(f"  - {table_name}: {row_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Preview

# COMMAND ----------

# Show sample data from key tables
key_tables = ["raw_blinkit_orders", "raw_blinkit_customers", "raw_blinkit_delivery_performance"]

for table in key_tables:
    print(f"\n📊 Sample data from {table}:")
    spark.sql(f"SELECT * FROM {dq_agent_catalog_name}.{dq_agent_raw_schema_name}.{table} LIMIT 3").show(truncate=False) 