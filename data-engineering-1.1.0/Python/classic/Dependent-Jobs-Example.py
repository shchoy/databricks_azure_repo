# Databricks notebook source
# MAGIC %md
# MAGIC ### Example of scheduling different dependency notebooks. Schedule this Notebook to run the dependencies.

# COMMAND ----------

result = dbutils.notebook.run("./steps/raw-to-bronze")
assert result

# COMMAND ----------

result = dbutils.notebook.run("./steps/bronze-to-silver")
assert result

# COMMAND ----------

result = dbutils.notebook.run("./steps/silver-to-gold")
assert result