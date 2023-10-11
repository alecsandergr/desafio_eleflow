# Databricks notebook source
pip install python-dotenv

# COMMAND ----------

import os
from dotenv import load_dotenv

# COMMAND ----------

dbfs_path = 'dbfs:/eleflow/config/01.raw'
local_path = dbfs_path.replace('dbfs:/', 'file:/tmp/')
dbutils.fs.cp(dbfs_path, local_path, recurse=True)

# COMMAND ----------

# dbutils.fs.ls('dbfs:/eleflow/datalake/landing/VRA/json/')

# COMMAND ----------

load_dotenv('/tmp/eleflow/config/01.raw/.env')
API_KEY = os.getenv('API_KEY')