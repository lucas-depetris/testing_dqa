# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Transform and Clean Data
# MAGIC
# MAGIC This notebook transforms and cleans the raw data, applying business rules and data quality improvements.

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
\n\n# Test modification from github.py test