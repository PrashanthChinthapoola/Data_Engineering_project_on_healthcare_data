# Databricks notebook source
# Set up Azure Storage credentials
#storage_account_name = "uhgstoragee"
#storage_account_key = "Dh5cvpN+yHtULbgFjHTj5gCw056yfq2QiNC9Kyvv8vfFFAidX37GjD/mjr9XA1r4axUMWIHqVbqa+AStQeS17Q==" 
#container_name = "uhgdata"
spark.conf.set("fs.azure.account.auth.type.uhgstoragee.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.uhgstoragee.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.uhgstoragee.dfs.core.windows.net", 'Dh5cvpN+yHtULbgFjHTj5gCw056yfq2QiNC9Kyvv8vfFFAidX37GjD/mjr9XA1r4axUMWIHqVbqa+AStQeS17Q==')

# COMMAND ----------

# Try listing the files in the container
dbutils.fs.ls("abfss://uhgdata@uhgstoragee.dfs.core.windows.net") 


# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Read JSON from Azure datalake").getOrCreate()
json_file_path = "abfss://uhgdata@uhgstoragee.dfs.core.windows.net/*.json"
# Read the JSON file with multiline option
df = spark.read.option("multiline", "true").json(json_file_path)
# Show the DataFrame
display(df)


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Function to flatten the JSON structure
def hpt_json(df):
    # Explode 'reporting_structure' to create one row per reporting plan
    df = df.withColumn("reporting_structure", F.explode(df["reporting_structure"]))
    
    # Explode 'reporting_plans' to create one row per plan
    df = df.withColumn("reporting_plans", F.explode(df["reporting_structure"]["reporting_plans"]))
    
    # Explode 'in_network_files' to create one row per in-network file
    df = df.withColumn("in_network_files", F.explode(df["reporting_structure"]["in_network_files"]))
    
    # Select and rename relevant columns
    df = df.select(
        "reporting_entity_name",
        "reporting_entity_type",
        F.col("reporting_plans.plan_name").alias("plan_name"),
        F.col("reporting_plans.plan_id").cast("integer").alias("plan_id"),
        F.col("reporting_plans.plan_id_type").alias("plan_id_type"),
        F.col("reporting_plans.plan_market_type").alias("plan_market_type"),
        F.col("in_network_files.description").alias("file_description"),
        F.col("in_network_files.location").alias("file_location")
    )
    
    return df
    # Flatten the DataFrame using the defined function
hpt_df = hpt_json(df)

# Display the flattened structure
display(hpt_df)
display(hpt_df.count())

# COMMAND ----------

output_path = "abfss://output@uhgstoragee.dfs.core.windows.net/uhg_finaldata"
hpt_df.coalesce(1).write.format("parquet").mode("overwrite").save(output_path)

# COMMAND ----------

# Snowflake connection options
snowflake_options = {
    "sfURL": "https://ue23408.north-europe.azure.snowflakecomputing.com", 
    "sfUser": "PRASHANTH",
    "sfPassword": "Azure@123",
    "sfDatabase": "UHG_DATABASE",
    "sfSchema": "UHG_SCHEMA",
    "sfWarehouse": "COMPUTE_WH",
}

# Write DataFrame to Snowflake
hpt_df.write.format("snowflake").options(**snowflake_options)\
    .option("dbtable", "HPT").mode("append").save()
print("Data successfully written to Snowflake!")

