# Databricks notebook source
from pathlib import Path

from pyspark.sql import SparkSession

from tests.iterable.common_dbricks import setup_schema, load_test_files, show_tables
from tests.iterable.iterable_helper import IterableHelper
from tests.iterable.test_load_events_into_iterable import test_load_events_into_iterable
from tests.iterable.test_load_user_profile_into_iterable import test_load_user_profile_into_iterable
from tests.iterable.test_load_tasks_into_iterable import test_load_tasks_into_iterable
import pandas as pd



# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE SCHEMA IF NOT EXISTS `hive_metastore`.`personalization`
# MAGIC DROP DATABASE IF EXISTS `hive_metastore`.`business_events` CASCADE;

# COMMAND ----------

spark.sql("USE hive_metastore.personalization")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load User Profile into Iterable

# COMMAND ----------

test_load_user_profile_into_iterable(spark) #if only it were so simple

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Picking up where the failed code left off

# COMMAND ----------

my_csv = "/Workspace/Repos/kenan.spruill@icanbwell.com/SparkPipelineFramework/tests/iterable/test_files/user_profile.csv"
dfff = pd.read_csv(my_csv)
dfff

my_csv2 = f"file:{my_csv}"

dfc = spark.read.csv(
    my_csv2,
    header=True,
    inferSchema=True,
)
display(dfc)


# COMMAND ----------


# Assuming other imports are correct and necessary for your context

# Convert the string path to a Path object
data_dir = Path("/Workspace/Repos/kenan.spruill@icanbwell.com/SparkPipelineFramework/tests/iterable")
print("Data Directory: ", data_dir)

# Now you can safely use setup_schema, load_test_files, and show_tables with the corrected data_dir
# setup_schema(spark_session)
load_test_files(data_dir, spark)



# COMMAND ----------

show_tables(spark)

# COMMAND ----------

"/Workspace/Repos/kenan.spruill@icanbwell.com/SparkPipelineFramework/tests/iterable/test_files/user_profile.csv"


# COMMAND ----------

# MAGIC %md # Load Events into Iterable

# COMMAND ----------

# run code
test_load_events_into_iterable(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Tasks into Iterable

# COMMAND ----------

test_load_tasks_into_iterable(spark)
