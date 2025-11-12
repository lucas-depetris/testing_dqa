# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Transform and Clean Data
# MAGIC
# MAGIC This notebook transforms and cleans the raw data, applying business rules and data quality improvements.
# MAGIC
# MAGIC ## What this notebook does:
# MAGIC - Applies data cleaning and standardization
# MAGIC - Handles missing values and data type conversions
# MAGIC - Applies business rules and transformations
# MAGIC - Creates processed tables ready for final processing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import re
import os
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Configuration

# COMMAND ----------

dq_agent_catalog_name = dbutils.widgets.get("dq_agent_catalog_name")
dq_agent_schema_name = dbutils.widgets.get("dq_agent_schema_name")
dq_agent_raw_schema_name = dbutils.widgets.get("dq_agent_raw_schema_name")
dq_agent_processed_schema_name = dbutils.widgets.get("dq_agent_processed_schema_name")
dq_agent_final_schema_name = dbutils.widgets.get("dq_agent_final_schema_name")
csv_folder_path = dbutils.widgets.get("csv_folder_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleaning Functions

# COMMAND ----------

def clean_string_column(df, column_name):
    """Clean string columns by trimming whitespace and handling nulls"""
    return df.withColumn(
        column_name,
        when(col(column_name).isNotNull(), trim(col(column_name)))
        .otherwise(lit(None))
    )

def standardize_email(df, email_column):
    """Standardize email format"""
    return df.withColumn(
        email_column,
        when(col(email_column).isNotNull(),
             lower(trim(col(email_column))))
        .otherwise(lit(None))
    )

def clean_phone_number(df, phone_column):
    """Clean and standardize phone numbers"""
    return df.withColumn(
        phone_column,
        when(col(phone_column).isNotNull(),
             regexp_replace(col(phone_column), r'[^\\d+]', ''))
        .otherwise(lit(None))
    )

def validate_numeric_range(df, column_name, min_val=None, max_val=None):
    """Validate numeric columns within specified range"""
    condition = col(column_name).isNotNull()
    if min_val is not None:
        condition = condition & (col(column_name) >= min_val)
    if max_val is not None:
        condition = condition & (col(column_name) <= max_val)
    
    return df.withColumn(
        column_name,
        when(condition, col(column_name))
        .otherwise(lit(None))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Orders Data

# COMMAND ----------

print("🔄 Processing orders data...")

# Read raw orders data
orders_df = spark.sql(f"SELECT * FROM {dq_agent_catalog_name}.{dq_agent_raw_schema_name}.raw_blinkit_orders")

# Apply transformations
processed_orders = orders_df \
    .withColumn("order_date", to_timestamp("order_date")) \
    .withColumn("promised_delivery_time", to_timestamp("promised_delivery_time")) \
    .withColumn("actual_delivery_time", to_timestamp("actual_delivery_time")) \
    .withColumn("order_total", round(col("order_total"), 2)) \
    .withColumn("delivery_status", 
                when(col("delivery_status").isNull(), "Unknown")
                .otherwise(initcap(col("delivery_status")))) \
    .withColumn("payment_method", 
                when(col("payment_method").isNull(), "Unknown")
                .otherwise(initcap(col("payment_method")))) \
    .withColumn("_processed_timestamp", current_timestamp()) \
    .withColumn("_data_quality_score", lit(1.0))  # Placeholder for quality scoring

# Validate numeric ranges
processed_orders = validate_numeric_range(processed_orders, "order_total", min_val=0)
processed_orders = validate_numeric_range(processed_orders, "store_id", min_val=1)

# Add derived columns
processed_orders = processed_orders \
    .withColumn("delivery_time_minutes", 
                when(col("actual_delivery_time").isNotNull() & col("promised_delivery_time").isNotNull(),
                     round((unix_timestamp("actual_delivery_time") - unix_timestamp("promised_delivery_time")) / 60, 2))
                .otherwise(lit(None))) \
    .withColumn("is_on_time", 
                when(col("delivery_status") == "On Time", True)
                .otherwise(False)) \
    .withColumn("order_month", date_format("order_date", "yyyy-MM")) \
    .withColumn("order_year", year("order_date"))

print(f"✅ Processed orders: {processed_orders.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Customers Data

# COMMAND ----------

print("🔄 Processing customers data...")

# Read raw customers data
customers_df = spark.sql(f"SELECT * FROM {dq_agent_catalog_name}.{dq_agent_raw_schema_name}.raw_blinkit_customers")

# Apply transformations
processed_customers = customers_df \
    .transform(lambda df: clean_string_column(df, "customer_name")) \
    .transform(lambda df: clean_string_column(df, "address")) \
    .transform(lambda df: clean_string_column(df, "area")) \
    .transform(lambda df: standardize_email(df, "email")) \
    .transform(lambda df: clean_phone_number(df, "phone")) \
    .withColumn("registration_date", to_date("registration_date")) \
    .withColumn("customer_segment", 
                when(col("customer_segment").isNull(), "Unknown")
                .otherwise(initcap(col("customer_segment")))) \
    .withColumn("total_orders", 
                when(col("total_orders").isNull(), 0)
                .otherwise(col("total_orders"))) \
    .withColumn("avg_order_value", 
                when(col("avg_order_value").isNull(), 0.0)
                .otherwise(round(col("avg_order_value"), 2))) \
    .withColumn("_processed_timestamp", current_timestamp()) \
    .withColumn("_data_quality_score", lit(1.0))

# Validate numeric ranges
processed_customers = validate_numeric_range(processed_customers, "total_orders", min_val=0)
processed_customers = validate_numeric_range(processed_customers, "avg_order_value", min_val=0)
processed_customers = validate_numeric_range(processed_customers, "pincode", min_val=100000, max_val=999999)

# Add derived columns
processed_customers = processed_customers \
    .withColumn("customer_age_days", 
                when(col("registration_date").isNotNull(),
                     datediff(current_date(), col("registration_date")))
                .otherwise(lit(None))) \
    .withColumn("is_premium", 
                when(col("customer_segment") == "Premium", True)
                .otherwise(False)) \
    .withColumn("registration_year", year("registration_date"))

print(f"✅ Processed customers: {processed_customers.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Delivery Performance Data

# COMMAND ----------

print("🔄 Processing delivery performance data...")

# Read raw delivery performance data
delivery_df = spark.sql(f"SELECT * FROM {dq_agent_catalog_name}.{dq_agent_raw_schema_name}.raw_blinkit_delivery_performance")

# Apply transformations
processed_delivery = delivery_df \
    .withColumn("promised_time", to_timestamp("promised_time")) \
    .withColumn("actual_time", to_timestamp("actual_time")) \
    .withColumn("delivery_time_minutes", 
                when(col("delivery_time_minutes").isNotNull(), round(col("delivery_time_minutes"), 2))
                .otherwise(lit(None))) \
    .withColumn("distance_km", 
                when(col("distance_km").isNotNull(), round(col("distance_km"), 2))
                .otherwise(lit(None))) \
    .withColumn("delivery_status", 
                when(col("delivery_status").isNull(), "Unknown")
                .otherwise(initcap(col("delivery_status")))) \
    .withColumn("reasons_if_delayed", 
                when(col("reasons_if_delayed").isNull(), "None")
                .otherwise(trim(col("reasons_if_delayed")))) \
    .withColumn("_processed_timestamp", current_timestamp()) \
    .withColumn("_data_quality_score", lit(1.0))

# Validate numeric ranges
processed_delivery = validate_numeric_range(processed_delivery, "delivery_time_minutes", min_val=-60, max_val=300)
processed_delivery = validate_numeric_range(processed_delivery, "distance_km", min_val=0, max_val=50)

# Add derived columns
processed_delivery = processed_delivery \
    .withColumn("is_delayed", 
                when(col("delivery_time_minutes") > 0, True)
                .otherwise(False)) \
    .withColumn("delay_category", 
                when(col("delivery_time_minutes") <= 0, "On Time")
                .when(col("delivery_time_minutes") <= 5, "Slight Delay")
                .when(col("delivery_time_minutes") <= 15, "Moderate Delay")
                .otherwise("Significant Delay")) \
    .withColumn("delivery_efficiency", 
                when(col("distance_km") > 0,
                     round(col("delivery_time_minutes") / col("distance_km"), 2))
                .otherwise(lit(None)))

print(f"✅ Processed delivery performance: {processed_delivery.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Processed Tables

# COMMAND ----------

print("💾 Saving processed tables...")

# Save processed orders
processed_orders.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_processed_schema_name}.processed_blinkit_orders")
print(f"✅ Saved processed orders to {dq_agent_catalog_name}.{dq_agent_processed_schema_name}.processed_blinkit_orders")

# Save processed customers
processed_customers.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_processed_schema_name}.processed_blinkit_customers")
print(f"✅ Saved processed customers to {dq_agent_catalog_name}.{dq_agent_processed_schema_name}.processed_blinkit_customers")

# Save processed delivery performance
processed_delivery.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_processed_schema_name}.processed_blinkit_delivery_performance")
print(f"✅ Saved processed delivery performance to {dq_agent_catalog_name}.{dq_agent_processed_schema_name}.processed_blinkit_delivery_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

# Generate data quality summary
print("📊 Data Quality Summary:")
print("=" * 50)

# Orders quality metrics
orders_quality = processed_orders.select(
    count("*").alias("total_rows"),
    count("order_id").alias("non_null_order_ids"),
    count("customer_id").alias("non_null_customer_ids"),
    count("order_total").alias("non_null_order_totals"),
    count("is_on_time").alias("delivery_status_available")
).collect()[0]

print(f"Orders Table:")
print(f"  - Total rows: {orders_quality['total_rows']}")
print(f"  - Valid order IDs: {orders_quality['non_null_order_ids']}")
print(f"  - Valid customer IDs: {orders_quality['non_null_customer_ids']}")
print(f"  - Valid order totals: {orders_quality['non_null_order_totals']}")
print(f"  - Delivery status available: {orders_quality['delivery_status_available']}")

# Customers quality metrics
customers_quality = processed_customers.select(
    count("*").alias("total_rows"),
    count("customer_id").alias("non_null_customer_ids"),
    count("email").alias("valid_emails"),
    count("phone").alias("valid_phones"),
    count("customer_segment").alias("segments_available")
).collect()[0]

print(f"\nCustomers Table:")
print(f"  - Total rows: {customers_quality['total_rows']}")
print(f"  - Valid customer IDs: {customers_quality['non_null_customer_ids']}")
print(f"  - Valid emails: {customers_quality['valid_emails']}")
print(f"  - Valid phones: {customers_quality['valid_phones']}")
print(f"  - Segments available: {customers_quality['segments_available']}")

# Delivery quality metrics
delivery_quality = processed_delivery.select(
    count("*").alias("total_rows"),
    count("order_id").alias("non_null_order_ids"),
    count("delivery_time_minutes").alias("delivery_times_available"),
    count("distance_km").alias("distances_available"),
    count("is_delayed").alias("delay_status_available")
).collect()[0]

print(f"\nDelivery Performance Table:")
print(f"  - Total rows: {delivery_quality['total_rows']}")
print(f"  - Valid order IDs: {delivery_quality['non_null_order_ids']}")
print(f"  - Delivery times available: {delivery_quality['delivery_times_available']}")
print(f"  - Distances available: {delivery_quality['distances_available']}")
print(f"  - Delay status available: {delivery_quality['delay_status_available']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Processed Data

# COMMAND ----------

# Show sample processed data
print("📋 Sample Processed Data:")
print("\nProcessed Orders:")
processed_orders.select("order_id", "customer_id", "order_total", "delivery_status", "is_on_time", "order_month").show(5)

print("\nProcessed Customers:")
processed_customers.select("customer_id", "customer_name", "customer_segment", "total_orders", "is_premium").show(5)

print("\nProcessed Delivery Performance:")
processed_delivery.select("order_id", "delivery_time_minutes", "distance_km", "is_delayed", "delay_category").show(5) 
