# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup and Configuration
# MAGIC
# MAGIC This notebook sets up the environment and creates necessary schemas for the Blinkit data pipeline.
# MAGIC
# MAGIC ## What this notebook does:
# MAGIC - Installs required packages
# MAGIC - Sets up Unity Catalog configuration
# MAGIC - Creates necessary schemas and catalogs
# MAGIC - Defines global variables and configurations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Variables

# COMMAND ----------

# Configuration variables
#catalog_name = "unity_catalog_testing"
dq_agent_catalog_name = "dq_agent_testing"
dq_agent_schema_name = "data_quality_agent_testing"
dq_agent_raw_schema_name = "dqa_raw_data"
dq_agent_processed_schema_name = "dqa_processed_data"
dq_agent_final_schema_name = "dqa_final_data"

# File paths
csv_folder_path = "/Volumes/dq_agent_testing/raw_csvs/files"

# Table names
raw_tables = {
    "orders": "raw_blinkit_orders",
    "customers": "raw_blinkit_customers", 
    "delivery_performance": "raw_blinkit_delivery_performance",
    "products": "raw_blinkit_products",
    "order_items": "raw_blinkit_order_items",
    "inventory": "raw_blinkit_inventory",
    "marketing_performance": "raw_blinkit_marketing_performance",
    "customer_feedback": "raw_blinkit_customer_feedback"
}

processed_tables = {
    "orders": "processed_blinkit_orders",
    "customers": "processed_blinkit_customers",
    "delivery_performance": "processed_blinkit_delivery_performance"
}

final_tables = {
    "orders": "final_blinkit_orders",
    "customers": "final_blinkit_customers", 
    "delivery_performance": "final_blinkit_delivery_performance"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalogs and Schemas

# COMMAND ----------

# Create schemas
schemas_to_create = [dq_agent_raw_schema_name, dq_agent_processed_schema_name, dq_agent_final_schema_name]

for schema in schemas_to_create:
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {dq_agent_catalog_name}.{schema}")
        print(f"✅ Schema '{dq_agent_catalog_name}.{schema}' created/verified")
    except Exception as e:
        print(f"⚠️ Schema creation issue for {schema}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

#List all schemas in the catalog
print("📋 Available schemas:")
schemas = spark.sql(f"SHOW SCHEMAS IN {dq_agent_catalog_name}").collect()
for row in schemas:
    print(f"  - {row[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up widget parameters for pipeline control

# COMMAND ----------

# Create widgets for pipeline control
dbutils.widgets.text("run_mode", "full", "Run Mode (full/incremental)")
dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("quality_checks", "true", ["true", "false"], "Run Quality Checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_widget_value(widget_name, default_value=""):
    """Get widget value with default fallback"""
    try:
        value = dbutils.widgets.get(widget_name)
        return value if value else default_value
    except:
        return default_value

def log_step(step_name, status="INFO"):
    """Log pipeline steps"""
    timestamp = spark.sql("SELECT current_timestamp() as ts").collect()[0]["ts"]
    print(f"[{timestamp}] {status}: {step_name}")