# Databricks notebook source
import os
from typing import Literal

# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps:
# MAGIC - Create the paths you need
# MAGIC - Mirror the path required into the following structure
# MAGIC   `'dbfs:/FileStore/shared_uploads/{your_email}/{path}'`
# MAGIC - Add the files into the path in the FileStore
# MAGIC - Move the files to the paths you need

# COMMAND ----------

def create_dirs(path: str) -> None:
    """
    Checks if path exists and if not creates it.

    Parameters
    ----------
    path: str
        The path to be created/checked.
    
    Returns
    -------
    None
    """

    try:
        dbutils.fs.ls(path)
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            dbutils.fs.mkdirs(path)

suffixes = ['dbfs:', 'dbfs:/FileStore/shared_uploads/alecsandergr6@gmail.com']

paths = ['eleflow/datalake/landing/AIR_CIA/csv',
         'eleflow/datalake/landing/VRA/json',
         'eleflow/config/01.raw', 
         'eleflow/config/02.bronze']

# Create all the necessary directories
for path in paths:
    for suffix in suffixes:
        fp = f'{suffix}/{path}'
        create_dirs(fp)
        check_path = '/'.join(fp.split('/')[:-1])
        #print(dbutils.fs.ls(check_path))

# Moves all the files
for path in paths:
    nb_bf_running = dbutils.fs.ls(f'{suffixes[0]}/{path}')
    print(f'Number of files before script: {len(nb_bf_running)}')

    dbutils.fs.cp(f'{suffixes[1]}/{path}', f'{suffixes[0]}/{path}', recurse=True)

    nb_af_running = dbutils.fs.ls(f'{suffixes[0]}/{path}')
    print(f'Number of files after script: {len(nb_af_running)}')

# Creates the .env file
dbutils.fs.put('dbfs:/eleflow/config/01.raw/.env', "API_KEY=a9129ee409msh9c230e2905c0848p1ecd6fjsn191243e8ce97")

# COMMAND ----------

dbutils.fs.cp('dbfs:/FileStore/shared_uploads/alecsandergr6@gmail.com/eleflow/config/01.raw/config.json',
              'dbfs:/eleflow/config/01.raw/config.json'
              )