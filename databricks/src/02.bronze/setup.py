# Databricks notebook source
# file_path = 'dbfs:/FileStore/shared_uploads/alecsandergr6@gmail.com/eleflow/config/02.bronze/dbtools.py'
# new_path = file_path.replace('/FileStore/shared_uploads/alecsandergr6@gmail.com', '')
# dbutils.fs.cp(file_path, new_path)

# COMMAND ----------

#file_path = 'dbfs:/FileStore/shared_uploads/alecsandergr6@gmail.com/eleflow/config/02.bronze/config.json'
#new_path = file_path.replace('/FileStore/shared_uploads/alecsandergr6@gmail.com', '')
#dbutils.fs.cp(file_path, new_path)

# COMMAND ----------

dbfs_path = 'dbfs:/eleflow/config/02.bronze'
local_path = dbfs_path.replace('dbfs:/', 'file:/tmp/')
dbutils.fs.cp(dbfs_path, local_path, recurse=True)

# COMMAND ----------

import sys
sys.path.append(local_path.replace('file:', ''))