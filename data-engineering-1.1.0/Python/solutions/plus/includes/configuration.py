# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Define Data Paths.

# COMMAND ----------

# ANSWER
username = "dbacademy"

# COMMAND ----------

plusPipelinePath = f"/dbacademy/{username}/dataengineering/plus/"

rawPath = plusPipelinePath + "raw/"
bronzePath = plusPipelinePath + "bronze/"
silverPath = plusPipelinePath + "silver/"
goldPath = plusPipelinePath + "gold/"

checkpointPath = plusPipelinePath + "checkpoints/"
bronzeCheckpoint = checkpointPath + "bronze/"
silverCheckpoint = checkpointPath + "silver/"
goldCheckpoint = checkpointPath + "gold/"

# COMMAND ----------

# MAGIC %md
# MAGIC Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC Import Utility Functions

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

streams_stopped = stop_all_streams()

if streams_stopped:
    print("All streams stopped.")
else:
    print("No running streams.")
