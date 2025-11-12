# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Data Quality Checks
# MAGIC
# MAGIC This notebook implements comprehensive data quality validation using the Databricks DQX framework.
# MAGIC
# MAGIC ## What this notebook does:
# MAGIC - Implements data quality rules and checks
# MAGIC - Validates data integrity and business rules
# MAGIC - Separates valid and quarantined data
# MAGIC - Generates quality reports

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx
# MAGIC %restart_python

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.check_funcs import sql_query, is_unique, is_not_null
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import OutputConfig
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
# MAGIC ## Initialize DQ Engine

# COMMAND ----------

# Initialize DQ Engine
dq_engine = DQEngine(WorkspaceClient())
print("✅ DQ Engine initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Data Quality Rules

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders Quality Rules

# COMMAND ----------

# Read processed orders data
orders_df = spark.sql(f"SELECT * FROM {dq_agent_catalog_name}.{dq_agent_processed_schema_name}.processed_blinkit_orders")

# Define comprehensive orders quality checks
orders_checks = [
    # Basic uniqueness checks
    DQDatasetRule(
        criticality="error",
        check_func=check_funcs.is_unique,
        columns=["order_id"],
        #msg="Order ID must be unique",
        name="unique_order_id"
    ),
    
    # Composite key uniqueness (order_id + customer_id)
    DQDatasetRule(
        criticality="error",
        name="composite_key_order_customer_is_unique",
        check_func=check_funcs.is_unique,
        columns=["order_id", "customer_id"]#,
        #msg="Order ID and Customer ID combination must be unique"
    ),
    
    # Not null checks - check each required field individually
    DQRowRule(
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="order_id",
        #msg="Order ID cannot be null",
        name="order_id_not_null"
    ),
    
    DQRowRule(
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="customer_id",
        #msg="Customer ID cannot be null",
        name="customer_id_not_null"
    ),
    
    DQRowRule(
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="order_date",
        #msg="Order date cannot be null",
        name="order_date_not_null"
    ),
    
    # Range checks
    DQRowRule(
        criticality="warn",
        check_func=check_funcs.is_in_range,
        check_func_kwargs={
            "column": "order_total",
            "min_limit": 0,
            "max_limit": 100000
        },
        #msg="Order total should be between 0 and 100,000",
        name="order_total_range"
    ),
    
    # Aggregation checks - ensure minimum order count
    DQDatasetRule(
        criticality="error",
        check_func=check_funcs.is_aggr_not_less_than,
        column="*",
        check_func_kwargs={
            "aggr_type": "count",
            "limit": 1
        },
        #msg="Must have at least 1 order",
        name="minimum_order_count"
    ),
    
    # Aggregation checks - ensure reasonable order count per customer
    DQDatasetRule(
        criticality="warn",
        check_func=check_funcs.is_aggr_not_greater_than,
        column="order_id",
        check_func_kwargs={
            "aggr_type": "count",
            "group_by": ["customer_id"],
            "limit": 100
        },
        #msg="Customer should not have more than 100 orders",
        name="max_orders_per_customer"
    ),
    
    # Business logic checks using SQL
    DQDatasetRule(
        criticality="warn",
        check_func=check_funcs.sql_query,
        check_func_kwargs={
            "query": """
            SELECT order_id,
                   CASE WHEN promised_delivery_time < order_date THEN True ELSE False END as condition
            FROM {{ input_view }}
            WHERE promised_delivery_time IS NOT NULL AND order_date IS NOT NULL
            """,
            "merge_columns": ["order_id"],
            "condition_column": "condition",
            "input_placeholder": "input_view"
        },
        name="delivery_time_after_order"
    ),

    DQDatasetRule(
        criticality="warn",
        check_func=check_funcs.sql_query,
        check_func_kwargs={
            "query": """
            SELECT order_id,
                   CASE WHEN promised_delivery_time < order_date THEN True ELSE False END as condition
            FROM {{ input_view }}
            WHERE promised_delivery_time IS NOT NULL AND order_date IS NOT NULL
            """,
            "condition_column": "condition",
            "merge_columns": ["order_id"],
            "input_placeholder": "input_view"
        },
        name="delivery_time_after_order"
    ),
    
    # Data freshness check - ensure orders arrive within time windows
    DQDatasetRule(
        criticality="warn",
        check_func=check_funcs.is_data_fresh_per_time_window,
        column="order_date",
        check_func_kwargs={
            "window_minutes": 60,
            "min_records_per_window": 1,
            "lookback_windows": 24
        },
        #msg="Orders should arrive regularly within time windows",
        name="order_data_freshness"
    )
]

print(f"✅ Defined {len(orders_checks)} quality checks for orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers Quality Rules

# COMMAND ----------

# Read processed customers data
customers_df = spark.sql(f"SELECT * FROM {dq_agent_catalog_name}.{dq_agent_processed_schema_name}.processed_blinkit_customers")

# Define comprehensive customers quality checks
customers_checks = [
    # Basic uniqueness checks
    DQDatasetRule(
        criticality="error",
        check_func=check_funcs.is_unique,
        columns=["customer_id"],
        #msg="Customer ID must be unique",
        name="unique_customer_id"
    ),
    
    # Email uniqueness check
    DQDatasetRule(
        criticality="warn",
        check_func=check_funcs.is_unique,
        columns=["email"],
        #msg="Email addresses should be unique",
        name="unique_email_addresses"
    ),
    
    # Not null checks for customer_id
    DQRowRule(
        criticality="error",
        check_func=check_funcs.is_not_null,
        check_func_kwargs={
            "column": "customer_id"
        },
        #msg="Customer ID cannot be null",
        name="customer_id_required"
    ),
    
    # Not null checks for customer_name
    DQRowRule(
        criticality="error",
        check_func=check_funcs.is_not_null,
        check_func_kwargs={
            "column": "customer_name"
        },
        #msg="Customer name cannot be null",
        name="customer_name_required"
    ),
    
    # Email format check using SQL
    DQDatasetRule(
        criticality="warn",
        check_func=check_funcs.sql_query,
        check_func_kwargs={
            "query": """
            SELECT customer_id,
                   CASE WHEN email IS NOT NULL AND email NOT REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' 
                        THEN True ELSE False END as condition
            FROM {{ input_view }}
            """,
            "condition_column": "condition",
            "merge_columns": ["customer_id"],
            #"msg": "Email format is invalid",
            "input_placeholder": "input_view"
        },
        name= "valid_email_format",
    ),
    
    # Phone number format check
    DQDatasetRule(
        criticality="warn",
        check_func=check_funcs.sql_query,
        check_func_kwargs={
            "query": """
            SELECT customer_id,
                   CASE WHEN phone IS NOT NULL AND length(phone) < 10 THEN True ELSE False END as condition
            FROM {{ input_view }}
            """,
            "condition_column": "condition",
            "merge_columns": ["customer_id"],
            #"msg": "Email format is invalid",
            "input_placeholder": "input_view"
        },
        name= "phone_number_length"
    ),
    
    # Customer segment validation
    DQDatasetRule(
        criticality="warn",
        check_func=check_funcs.sql_query,
        check_func_kwargs={
            "query": """
            SELECT customer_id,
                   CASE WHEN customer_segment NOT IN ('Premium', 'Regular', 'New', 'Inactive', 'Unknown') 
                        THEN True ELSE False END as condition
            FROM {{ input_view }}
            """,
            "condition_column": "condition",
            "merge_columns": ["customer_id"],
            #"msg": "Email format is invalid",
            "input_placeholder": "input_view"
        },
        name= "valid_customer_segment"
    ),
    
    # Aggregation checks - ensure reasonable customer count
    DQDatasetRule(
        criticality="error",
        check_func=check_funcs.is_aggr_not_less_than,
        column="*",
        check_func_kwargs={
            "aggr_type": "count",
            "limit": 1
        },
        #msg="Must have at least 1 customer",
        name="minimum_customer_count"
    ),
    
    # Customer segment distribution check
    DQDatasetRule(
        criticality="warn",
        check_func=check_funcs.sql_query,
        check_func_kwargs={
            "query": """
            SELECT customer_segment,
                   CASE WHEN COUNT(*) < 1 THEN True ELSE False END as condition
            FROM {{ input_view }}
            GROUP BY customer_segment
            """,
            "condition_column": "condition",
            "merge_columns": ["customer_segment"],
            #"msg": "Each customer segment should have at least 1 customer",
            "input_placeholder": "input_view"
        },
        name= "customer_segment_distribution"
    )
]

print(f"✅ Defined {len(customers_checks)} quality checks for customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delivery Performance Quality Rules

# COMMAND ----------

# Read processed delivery performance data
delivery_df = spark.sql(f"SELECT * FROM {dq_agent_catalog_name}.{dq_agent_processed_schema_name}.processed_blinkit_delivery_performance")

# Define delivery performance quality checks
delivery_checks = [
    # Uniqueness checks
    DQDatasetRule(
        criticality="error",
        check_func=is_unique,
        columns=["order_id"],
        #msg="Order ID must be unique in delivery performance",
        name="unique_delivery_order_id"
    ),
    
    # Not null checks
    DQRowRule(
        criticality="error",
        check_func=is_not_null,
        column="order_id",
        #msg="Order ID and delivery status cannot be null",
        name="order_id_required"
    ),
    # Not null checks
    DQRowRule(
        criticality="error",
        check_func=is_not_null,
        column="delivery_status",
        #msg="Order ID and delivery status cannot be null",
        name="delivery_status_required"
    ),
    
    # Distance range check
    DQRowRule(
        criticality="warn",
        check_func=check_funcs.is_in_range,
        check_func_kwargs={
            "column": "delivery_time_minutes",
            "min_limit": 60,
            "max_limit": 300
        },
        #msg="Order total should be between 0 and 100,000",
        name="order_total_range"
    ),

    # Distance range check
    DQRowRule(
        criticality="warn",
        check_func=check_funcs.is_in_range,
        check_func_kwargs={
            "column": "distance_km",
            "min_limit": 0,
            "max_limit": 50
        },
        #msg="Order total should be between 0 and 100,000",
        name="order_total_range"
    ),
    
    
    # Delivery status validation
    DQDatasetRule(
        criticality="warn",
        check_func=sql_query,
        check_func_kwargs={
            "query": """
            SELECT order_id,
                   CASE WHEN delivery_status NOT IN ('On Time', 'Delayed', 'Unknown') 
                        THEN True ELSE False END as condition
            FROM {{ delivery }}
            """,
            "condition_column": "condition",
            #"msg": "Invalid delivery status",
            "merge_columns": ["order_id"],
            "input_placeholder": "delivery"
        },
        name="delivery_status_validation"
    )
]

print(f"✅ Defined {len(delivery_checks)} quality checks for delivery performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Quality Checks

# COMMAND ----------

print("🔍 Running data quality checks...")
print("=" * 50)

# Run orders quality checks
print("\n📦 Checking orders data quality...")
orders_ref_dfs = {"orders": orders_df}
orders_valid_df, orders_quarantine_df = dq_engine.apply_checks_and_split(orders_df, orders_checks)

print(f"✅ Orders - Valid: {orders_valid_df.count()}, Quarantined: {orders_quarantine_df.count()}")

# Run customers quality checks
print("\n👥 Checking customers data quality...")
customers_ref_dfs = {"customers": customers_df}
customers_valid_df, customers_quarantine_df = dq_engine.apply_checks_and_split(customers_df, customers_checks)

print(f"✅ Customers - Valid: {customers_valid_df.count()}, Quarantined: {customers_quarantine_df.count()}")

# Run delivery performance quality checks
print("\n🚚 Checking delivery performance data quality...")
delivery_ref_dfs = {"delivery": delivery_df}
delivery_valid_df, delivery_quarantine_df = dq_engine.apply_checks_and_split(delivery_df, delivery_checks)

print(f"✅ Delivery Performance - Valid: {delivery_valid_df.count()}, Quarantined: {delivery_quarantine_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Valid and Quarantined Data

# COMMAND ----------


print("💾 Saving quality-checked data...")

# Save valid data
orders_valid_df.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.final_blinkit_orders")
customers_valid_df.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.final_blinkit_customers")
delivery_valid_df.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.final_blinkit_delivery_performance")

print("✅ Saved valid data to final tables")

# Save quarantined data
orders_quarantine_df.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.quarantine_blinkit_orders")
customers_quarantine_df.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.quarantine_blinkit_customers")
delivery_quarantine_df.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.quarantine_blinkit_delivery_performance")

print("✅ Saved quarantined data to quarantine tables")

# COMMAND ----------

import sys, importlib, inspect
sys.path.append("/Workspace/Shared/data_quality_agent/src")
import dqx_logger
from dqx_logger import DQXLogger
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DQX_Logger_Demo").getOrCreate()
log_table = "dq_agent_testing.dqa_final_data.dq_test_logs"
dq_logger = DQXLogger(spark, log_table)

# ORDERS
dq_logger.log_results(
    rules=orders_checks, 
    failed_records=orders_quarantine_df,
    errors_table=f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.quarantine_blinkit_orders"
)
# CUSTOMERS
dq_logger.log_results(
    rules=customers_checks,
    failed_records=customers_quarantine_df,
    errors_table=f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.quarantine_blinkit_customers"
)
# DELIVERY
dq_logger.log_results(
    rules=delivery_checks,
    failed_records=delivery_quarantine_df,
    errors_table=f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.quarantine_blinkit_delivery_performance"
)


# COMMAND ----------


#from databricks.labs.dqx.checks_serializer import serialize_checks
#
#def __init__(self, spark: SparkSession, table_name: str):
#    """
#    Initializes the DeltaQLogger with a Spark session and target table name.
#
#    Args:
#        spark (SparkSession): The SparkSession object to use for DataFrame operations.
#        table_name (str): The name of the Delta table where results will be stored.
#    """
#    self.spark = spark
#    self.table_name = table_name
#
#def log_results(self,
#                dqx_results: DataFrame,
#                test_definitions: List[Dict[str, Any]],
#                run_id: str,
#                valid_table: str = None,
#                quarantine_table: str = None):
#    """
#    Processes and logs the results of a DQX run to the target Delta table.
#
#    This method transforms the raw DQX results DataFrame to match the required
#    schema, joins it with the test definitions, and saves it.
#
#    Args:
#        dqx_results (DataFrame): The DataFrame produced by a DQX `dq.rules(...).check()` call.
#        test_definitions (List[Dict[str, Any]]): A list of dictionaries, where each
#                                                dictionary is a DQX rule definition.
#        run_id (str): A unique identifier for the pipeline or job run.
#        valid_table (str, optional): The name of the table where valid records were written. Defaults #to None.
#        quarantine_table (str, optional): The name of the table where invalid records were written. #Defaults to None.
#    """
#    print(f"Logging results for run_id: {run_id} to table: {self.table_name}")
#
#    # Create a DataFrame from the test definitions for easy joining
#    # The 'name' in the test definition corresponds to the 'rule' in dqx_results
#    schema = "name STRING, definition STRING"
#    defs_data = [(rule.get("name"), json.dumps(rule)) for rule in test_definitions]
#    defs_df = self.spark.createDataFrame(defs_data, schema)
#
#    # Transform the raw DQX results to the target schema
#    # DQX results have columns: rule, status, failed_records
#    results_df = dqx_results.withColumnRenamed("rule", "rule_name") \\
#                            .withColumnRenamed("status", "check_status") \\
#                            .withColumnRenamed("failed_records", "failed_records_count")
#
#    # Join results with definitions to add the detailed test_definition
#    enriched_results = results_df.join(
#        defs_df,
#        results_df["rule_name"] == defs_df["name"],
#        "left"
#    ).withColumnRenamed("definition", "test_definition")
#
#    # Add the remaining metadata columns
#    final_log = enriched_results.withColumn("run_id", lit(run_id)) \\
#                                .withColumn("run_timestamp", current_timestamp()) \\
#                                .withColumn("valid_table", lit(valid_table)) \\
#                                .withColumn("quarantine_table", lit(quarantine_table))
#
#    # Select columns in the desired final order and write to Delta table
#    output_df = final_log.select(
#        "run_id",
#        "run_timestamp",
#        "valid_table",
#        "quarantine_table",
#        "check_status",
#        "failed_records_count",
#        "test_definition"
#    )
#    
#    print("Schema of the final log DataFrame:")
#    output_df.printSchema()
#
#    print("Writing data to the Delta table...")
#    output_df.write.format("delta").mode("append").saveAsTable(self.table_name)
#    print("Successfully logged DQX results.")
#
#spark = SparkSession.builder.appName("DQX_Logger_Demo").getOrCreate()
#log_table_name = "dq_agent_testing.dqa_final_data.dq_test_logs"
#spark.sql(f"""
#    CREATE TABLE IF NOT EXISTS dq_agent_testing.dqa_final_data.dq_test_logs (
#        run_id STRING,
#        run_timestamp TIMESTAMP,
#        valid_table STRING,
#        quarantine_table STRING,
#        check_status STRING,
#        failed_records_count LONG,
#        test_definition STRING
#    ) USING DELTA
#""")
#dq_logger = DeltaQLogger(spark, log_table_name)
#
#from databricks.labs.dqx.checks_serializer import serialize_checks
#checks_dict: list[dict] = serialize_checks(customers_checks)
#log_results(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.quarantine_blinkit_customers", f"#{dq_agent_catalog_name}.{dq_agent_final_schema_name}.final_blinkit_orders", checks_dict)

    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Report

# COMMAND ----------

print("📊 Data Quality Report")
print("=" * 50)

# Calculate quality metrics
total_orders = orders_df.count()
valid_orders = orders_valid_df.count()
quarantined_orders = orders_quarantine_df.count()
orders_quality_score = (valid_orders / total_orders * 100) if total_orders > 0 else 0

total_customers = customers_df.count()
valid_customers = customers_valid_df.count()
quarantined_customers = customers_quarantine_df.count()
customers_quality_score = (valid_customers / total_customers * 100) if total_customers > 0 else 0

total_delivery = delivery_df.count()
valid_delivery = delivery_valid_df.count()
quarantined_delivery = delivery_quarantine_df.count()
delivery_quality_score = (valid_delivery / total_delivery * 100) if total_delivery > 0 else 0

print(f"\n📦 Orders Data Quality:")
print(f"  - Total records: {total_orders}")
print(f"  - Valid records: {valid_orders}")
print(f"  - Quarantined records: {quarantined_orders}")
print(f"  - Quality score: {orders_quality_score:.2f}%")

print(f"\n👥 Customers Data Quality:")
print(f"  - Total records: {total_customers}")
print(f"  - Valid records: {valid_customers}")
print(f"  - Quarantined records: {quarantined_customers}")
print(f"  - Quality score: {customers_quality_score:.2f}%")

print(f"\n🚚 Delivery Performance Data Quality:")
print(f"  - Total records: {total_delivery}")
print(f"  - Valid records: {valid_delivery}")
print(f"  - Quarantined records: {quarantined_delivery}")
print(f"  - Quality score: {delivery_quality_score:.2f}%")

# Overall quality score
total_records = total_orders + total_customers + total_delivery
total_valid = valid_orders + valid_customers + valid_delivery
overall_quality_score = (total_valid / total_records * 100) if total_records > 0 else 0

print(f"\n🎯 Overall Data Quality Score: {overall_quality_score:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantined Data Analysis

# COMMAND ----------

print("🔍 Quarantined Data Analysis")
print("=" * 50)

# Show sample quarantined data
if quarantined_orders > 0:
    print(f"\n📦 Sample quarantined orders ({quarantined_orders} total):")
    orders_quarantine_df.select("order_id", "customer_id", "order_total", "delivery_status").show(3)

if quarantined_customers > 0:
    print(f"\n👥 Sample quarantined customers ({quarantined_customers} total):")
    customers_quarantine_df.select("customer_id", "customer_name", "email", "customer_segment").show(3)

if quarantined_delivery > 0:
    print(f"\n🚚 Sample quarantined delivery records ({quarantined_delivery} total):")
    delivery_quarantine_df.select("order_id", "delivery_time_minutes", "distance_km", "delivery_status").show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Data Preview

# COMMAND ----------

print("✅ Final Data Preview")
print("=" * 50)

# Show sample final data
print("\n📦 Sample final orders:")
spark.sql(f"SELECT order_id, customer_id, order_total, delivery_status, order_month FROM {dq_agent_catalog_name}.{dq_agent_final_schema_name}.final_blinkit_orders LIMIT 5").show()

print("\n👥 Sample final customers:")
spark.sql(f"SELECT customer_id, customer_name, customer_segment, total_orders, is_premium FROM {dq_agent_catalog_name}.{dq_agent_final_schema_name}.final_blinkit_customers LIMIT 5").show()

print("\n🚚 Sample final delivery performance:")
spark.sql(f"SELECT order_id, delivery_time_minutes, distance_km, is_delayed, delay_category FROM {dq_agent_catalog_name}.{dq_agent_final_schema_name}.final_blinkit_delivery_performance LIMIT 5").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Quality Metrics

# COMMAND ----------

# Create quality metrics summary
quality_summary = spark.createDataFrame([
    ("orders", total_orders, valid_orders, quarantined_orders, orders_quality_score),
    ("customers", total_customers, valid_customers, quarantined_customers, customers_quality_score),
    ("delivery_performance", total_delivery, valid_delivery, quarantined_delivery, delivery_quality_score)
], ["table_name", "total_records", "valid_records", "quarantined_records", "quality_score"])

# Save quality summary
quality_summary.write.mode("overwrite").saveAsTable(f"{dq_agent_catalog_name}.{dq_agent_final_schema_name}.data_quality_summary")

print("✅ Quality metrics saved to data_quality_summary table")
print("🎉 Data quality checks completed successfully!") 