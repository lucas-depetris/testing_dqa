# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Create Final Tables
# MAGIC
# MAGIC This notebook creates the final analytical tables and implements the business logic from the original query.
# MAGIC
# MAGIC ## What this notebook does:
# MAGIC - Creates final analytical tables
# MAGIC - Implements the original business query logic
# MAGIC - Adds additional derived metrics and aggregations
# MAGIC - Creates summary tables for reporting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os

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
# MAGIC ## Create Final Analytical Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Main Business Query Table

# COMMAND ----------

print("🔄 Creating main business query table...")

# Execute the original business query
main_query = """
SELECT
  c.customer_segment,
  o.customer_id,
  dp.delivery_time_minutes,
  o.order_id,
  o.order_total,
  o.order_date,
  o.delivery_status,
  o.payment_method,
  c.customer_name,
  c.total_orders as customer_total_orders,
  c.avg_order_value as customer_avg_order_value,
  c.is_premium,
  dp.distance_km,
  dp.is_delayed,
  dp.delay_category,
  dp.delivery_efficiency,
  o.order_month,
  o.order_year
FROM
  {dq_agent_catalog_name}.{dq_agent_final_schema_name}.final_blinkit_orders AS o
LEFT JOIN
  {dq_agent_catalog_name}.{dq_agent_final_schema_name}.final_blinkit_customers AS c
  ON o.customer_id = c.customer_id
LEFT JOIN
  {dq_agent_catalog_name}.{dq_agent_final_schema_name}.final_blinkit_delivery_performance AS dp
  ON o.order_id = dp.order_id
""".format(dq_agent_catalog_name=dq_agent_catalog_name, dq_agent_final_schema_name=dq_agent_final_schema_name)

# Execute query and create table
main_analytics_df = spark.sql(main_query)

# Add additional derived columns
main_analytics_df = main_analytics_df \
    .withColumn("_created_timestamp", current_timestamp()) \
    .withColumn("_batch_id", lit(f"batch_{current_timestamp().cast('string')}"))

# Save to table
main_analytics_df.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_analytics_main")

print(f"✅ Created main analytics table with {main_analytics_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Customer Segment Analysis Table

# COMMAND ----------

print("🔄 Creating customer segment analysis table...")

customer_segment_analysis = main_analytics_df \
    .groupBy("customer_segment") \
    .agg(
        count("*").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("order_total").alias("avg_order_value"),
        sum("order_total").alias("total_revenue"),
        avg("delivery_time_minutes").alias("avg_delivery_time"),
        sum(when(col("is_delayed") == True, 1).otherwise(0)).alias("delayed_orders"),
        sum(when(col("is_delayed") == False, 1).otherwise(0)).alias("on_time_orders"),
        avg("distance_km").alias("avg_delivery_distance"),
        countDistinct("payment_method").alias("payment_methods_used")
    ) \
    .withColumn("delivery_success_rate", 
                round((col("on_time_orders") / col("total_orders")) * 100, 2)) \
    .withColumn("avg_orders_per_customer", 
                round(col("total_orders") / col("unique_customers"), 2)) \
    .withColumn("_created_timestamp", current_timestamp())

# Save to table
customer_segment_analysis.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_customer_segment_analysis")

print(f"✅ Created customer segment analysis table with {customer_segment_analysis.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Monthly Performance Summary

# COMMAND ----------

print("🔄 Creating monthly performance summary...")

monthly_performance = main_analytics_df \
    .groupBy("order_year", "order_month") \
    .agg(
        count("*").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("order_total").alias("total_revenue"),
        avg("order_total").alias("avg_order_value"),
        avg("delivery_time_minutes").alias("avg_delivery_time"),
        sum(when(col("is_delayed") == True, 1).otherwise(0)).alias("delayed_orders"),
        sum(when(col("is_delayed") == False, 1).otherwise(0)).alias("on_time_orders"),
        avg("distance_km").alias("avg_delivery_distance"),
        countDistinct("customer_segment").alias("segments_served")
    ) \
    .withColumn("delivery_success_rate", 
                round((col("on_time_orders") / col("total_orders")) * 100, 2)) \
    .withColumn("monthly_growth_rate", lit(None)) \
    .withColumn("_created_timestamp", current_timestamp()) \
    .orderBy("order_year", "order_month")

# Save to table
monthly_performance.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_monthly_performance")

print(f"✅ Created monthly performance table with {monthly_performance.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Delivery Performance Analysis

# COMMAND ----------

print("🔄 Creating delivery performance analysis...")

delivery_analysis = main_analytics_df \
    .groupBy("delay_category", "delivery_status") \
    .agg(
        count("*").alias("order_count"),
        avg("delivery_time_minutes").alias("avg_delivery_time"),
        avg("distance_km").alias("avg_distance"),
        avg("delivery_efficiency").alias("avg_efficiency"),
        sum("order_total").alias("total_revenue"),
        countDistinct("customer_id").alias("affected_customers")
    ) \
    .withColumn("percentage_of_orders", 
                round((col("order_count") / sum("order_count").over(Window.partitionBy())) * 100, 2)) \
    .withColumn("avg_order_value", 
                round(col("total_revenue") / col("order_count"), 2)) \
    .withColumn("_created_timestamp", current_timestamp())

# Save to table
delivery_analysis.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_delivery_analysis")

print(f"✅ Created delivery analysis table with {delivery_analysis.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Customer Performance Ranking

# COMMAND ----------

print("🔄 Creating customer performance ranking...")

# Define window for ranking
customer_window = Window.partitionBy("customer_segment").orderBy(col("total_revenue").desc())

customer_ranking = main_analytics_df \
    .groupBy("customer_id", "customer_name", "customer_segment", "is_premium") \
    .agg(
        count("*").alias("order_count"),
        sum("order_total").alias("total_revenue"),
        avg("order_total").alias("avg_order_value"),
        avg("delivery_time_minutes").alias("avg_delivery_time"),
        sum(when(col("is_delayed") == True, 1).otherwise(0)).alias("delayed_orders"),
        sum(when(col("is_delayed") == False, 1).otherwise(0)).alias("on_time_orders")
    ) \
    .withColumn("delivery_success_rate", 
                round((col("on_time_orders") / col("order_count")) * 100, 2)) \
    .withColumn("customer_rank_in_segment", rank().over(customer_window)) \
    .withColumn("customer_percentile", 
                round((col("customer_rank_in_segment") / max("customer_rank_in_segment").over(Window.partitionBy("customer_segment"))) * 100, 2)) \
    .withColumn("_created_timestamp", current_timestamp())

# Save to table
customer_ranking.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_customer_ranking")

print(f"✅ Created customer ranking table with {customer_ranking.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Payment Method Analysis

# COMMAND ----------

print("🔄 Creating payment method analysis...")

payment_analysis = main_analytics_df \
    .groupBy("payment_method") \
    .agg(
        count("*").alias("order_count"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("order_total").alias("total_revenue"),
        avg("order_total").alias("avg_order_value"),
        avg("delivery_time_minutes").alias("avg_delivery_time"),
        sum(when(col("is_delayed") == True, 1).otherwise(0)).alias("delayed_orders"),
        sum(when(col("is_delayed") == False, 1).otherwise(0)).alias("on_time_orders")
    ) \
    .withColumn("delivery_success_rate", 
                round((col("on_time_orders") / col("order_count")) * 100, 2)) \
    .withColumn("percentage_of_orders", 
                round((col("order_count") / sum("order_count").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) * 100, 2)) \
    .withColumn("_created_timestamp", current_timestamp())

# Save to table
payment_analysis.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_payment_analysis")

print(f"✅ Created payment analysis table with {payment_analysis.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Summary Dashboard Tables

# COMMAND ----------

print("📊 Creating summary dashboard tables...")

# Overall KPI summary - Convert all values to float to ensure consistent data types
kpi_summary = spark.createDataFrame([
    ("Total Orders", float(main_analytics_df.count())),
    ("Total Revenue", float(main_analytics_df.agg(sum("order_total")).collect()[0][0])),
    ("Unique Customers", float(main_analytics_df.select("customer_id").distinct().count())),
    ("Avg Order Value", float(main_analytics_df.agg(avg("order_total")).collect()[0][0])),
    ("Avg Delivery Time (minutes)", float(main_analytics_df.agg(avg("delivery_time_minutes")).collect()[0][0])),
    ("On-time Delivery Rate (%)", 
     float((main_analytics_df.filter(col("is_delayed") == False).count() / main_analytics_df.count()) * 100)),
    ("Premium Customers", float(main_analytics_df.filter(col("is_premium") == True).select("customer_id").distinct().count())),
    ("Total Segments", float(main_analytics_df.select("customer_segment").distinct().count()))
], ["metric", "value"])

kpi_summary.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_kpi_summary")

print("✅ Created KPI summary table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Table Summary

# COMMAND ----------

print("📋 Final Tables Created:")
print("=" * 50)

final_tables = [
    "blinkit_analytics_main",
    "blinkit_customer_segment_analysis", 
    "blinkit_monthly_performance",
    "blinkit_delivery_analysis",
    "blinkit_customer_ranking",
    "blinkit_payment_analysis",
    "blinkit_kpi_summary"
]

for table in final_tables:
    try:
        row_count = spark.sql(f"SELECT COUNT(*) as count FROM {dq_agent_catalog_name}.{dq_agent_final_schema_name}.{table}").collect()[0]["count"]
        print(f"✅ {table}: {row_count} rows")
    except Exception as e:
        print(f"❌ {table}: Error - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data from Final Tables

# COMMAND ----------

print("📊 Sample Data from Final Tables:")
print("=" * 50)

# Main analytics table
print("\n📦 Main Analytics Table:")
spark.sql(f"SELECT customer_segment, customer_id, order_total, delivery_time_minutes, is_delayed FROM {dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_analytics_main LIMIT 5").show()

# Customer segment analysis
print("\n👥 Customer Segment Analysis:")
spark.sql(f"SELECT * FROM {dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_customer_segment_analysis").show()

# KPI summary
print("\n📈 KPI Summary:")
spark.sql(f"SELECT * FROM {dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_kpi_summary").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Schema Documentation

# COMMAND ----------

print("📚 Table Schema Documentation:")
print("=" * 50)

# Document main analytics table schema
print("\n📦 blinkit_analytics_main:")
main_schema = spark.sql(f"SELECT * FROM {dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_analytics_main LIMIT 0")
for field in main_schema.schema.fields:
    print(f"  - {field.name}: {field.dataType}")

print("\n👥 blinkit_customer_segment_analysis:")
segment_schema = spark.sql(f"SELECT * FROM {dq_agent_catalog_name}.{dq_agent_final_schema_name}.blinkit_customer_segment_analysis LIMIT 0")
for field in segment_schema.schema.fields:
    print(f"  - {field.name}: {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Table Metadata

# COMMAND ----------

# Create table metadata summary
table_metadata = spark.createDataFrame([
    ("blinkit_analytics_main", "Main analytical table with all order, customer, and delivery data joined", "Analytical"),
    ("blinkit_customer_segment_analysis", "Aggregated metrics by customer segment", "Summary"),
    ("blinkit_monthly_performance", "Monthly performance metrics and trends", "Time Series"),
    ("blinkit_delivery_analysis", "Delivery performance analysis by delay categories", "Performance"),
    ("blinkit_customer_ranking", "Customer ranking and performance within segments", "Ranking"),
    ("blinkit_payment_analysis", "Payment method usage and performance analysis", "Payment"),
    ("blinkit_kpi_summary", "Key performance indicators summary", "KPI")
], ["table_name", "description", "category"])

table_metadata.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.table_metadata")

print("✅ Table metadata exported")
print("🎉 Final tables creation completed successfully!") 