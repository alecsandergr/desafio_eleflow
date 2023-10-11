# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

# MAGIC %run ../databases/create.sql

# COMMAND ----------

import dbtools
import os
import json
import delta
from pyspark.sql import types
from pyspark.sql.dataframe import DataFrame as pyspark_df
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import StreamingQuery
from typing import Literal, List

# COMMAND ----------

class BronzeIngestionAirport:
    """
    A class to load the data from airports.

    Attibutes
    ---------
    table_name : str
        The name of the table.
    database_name : str
        The name of the database.
    source : Literal['VRA', 'AIR_CIA', 'icaos']
        The source of the data.
    schema : types.StructType | None = None
        The schema from the table.
    read_options : dict
        Optional arguments to be passed when reading the file.
    partition_fields : List[str]
        A list of fields to partitionated the table.
    spark : spark
        A spark instance.
    file_format : str
        The format of the source file.
    path_full_load : str
        The path to retrieve all the data
    path_incremental : str
        The path to the incremental data
    id_fields : List[str]
        The primary keys of the table
    timestamp_field : str
        The field in which the table will be ordered by.
    table_full_name : str
        The combination of the database name and the table name
    checkpoint_path : str
        The path to the checkpoint

    Methods
    -------
    _make_dir(path: str) -> None :
        Create a directory.
    infer_schema() -> types.StructType :
        Loads a schema using the full table.
    load_schema() -> types.StructType | None :
        Tries to load the schema from the specified path.
    set_schema() -> types.StructType :
        Set the schema for the table. It tries to load from the path, otherwise it will infer the schema.
    save_schema() -> None :
        Saves the schema.
    load_full() -> pyspark_df :
        Loads the all the data from the path full load.
    save_full(, sdf: pyspark_df) -> None :
        Saves the data from the full_load path into a table.
    read_query() -> str | None :
        Reads the query from the path specified.
    default_query() -> str :
        Creates a default query using the primary keys and a order by clause.
        It filters duplicate values and returns only the most up to date.
    set_query() -> None :
        Sets the query for the table.
        It tries to load from the path, otherwise it will load the default.
    transform(table_name: str) -> pyspark_df :
        Apply the transformation to the table.
    process_full() -> None :
        Read and save the file contents into a table.
    load_stream() -> pyspark_df:
        Reads a stream.
    upsert(df: pyspark_df, delta_table: delta.tables.DeltaTable) -> None :
        Insert the data into the delta table if it does not already exist,
        otherwise update the delta table.
    save_stream(df_stream: pyspark_df) -> None :
        Save the stream into the table.
    process_stream() -> StreamingQuery :
        Process a streaming query, loading and saving the data.

    """

    def __init__(
            self,
            table_name: str,
            database_name: str,
            source: Literal['VRA', 'AIR_CIA', 'icaos'],
            schema: types.StructType | None = None,
            read_options: dict = {},
            partition_fields: List[str] = [],
            spark: SparkSession = spark
        ):
        """
        Constructs all the necessary atributes for the airport data object.

        Parameters
        ----------
        table_name : str
            The name of the table.
        database_name : str
            The name of the database.
        source : Literal['VRA', 'AIR_CIA', 'icaos']
            The source of the data.
        schema : types.StructType | None = None
            The schema from the table.
        read_options : dict
            Optional arguments to be passed when reading the file.
        partition_fields : List[str]
            A list of fields to partitionated the table.
        spark : spark
            A spark instance.
        """
        
        self.table_name = table_name
        self.database_name = database_name
        self.source = source
        self.schema = schema
        self.read_options = read_options
        self.partition_fields = partition_fields
        self.spark = spark

        with open('/tmp/eleflow/config/02.bronze/config.json') as f:
            config = json.load(f)

        self.file_format = config[source]['file_format']
        self.path_full_load = f"dbfs:/{config[source]['folder_path_history']}"
        self.path_incremental = f"dbfs:/{config[source]['folder_path_incremental']}"
        self.id_fields = config[source]['pks']
        self.timestamp_field = config[source]['order_by']

        self.table_full_name = f'{self.database_name}.{self.table_name}'
        self.checkpoint_path = f'{self.path_incremental.rstrip("/")}_{self.table_name}_checkpoint'

        self.set_schema()
        self.set_query()

    def _make_dir(path: str) -> None:
        """
        Create a directory.

        Parameters
        ----------
        path : str
            The path to create the directory.
        
        Returns
        -------
        None
        """
        try:
            dbutils.fs.mkdirs(path)
        except:
            pass

    def infer_schema(self) -> types.StructType:
        """
        Infers a schema using the full table.
        
        Returns
        -------
        types.StructType
            a schema for the table.
        """
        return self.load_full().schema
    
    def load_schema(self) -> types.StructType | None:
        """
        Tries to load the schema from the specified path.

        Returns
        -------
        types.StructType | None
            A schema for the table.    
        """
        
        schema_path_dbfs = f'dbfs:/datalake/eleflow/{self.source}/schema/{self.table_name}.json'
        schema_path_local = schema_path_dbfs.replace('dbfs:', '/tmp')
        try:
            dbutils.fs.cp(schema_path_dbfs, f'file:{schema_path_local}')
        except:
            pass

        if os.path.exists(schema_path_local):
            with open(schema_path_local) as f:
                return types.StructType.fromJson(json.load(f))            

    def set_schema(self) -> types.StructType:
        """
        Set the schema for the table.
        It tries to load from the path, otherwise it will infer the schema.

        Returns
        -------
        types.StructType
            A schema for the table.
        """

        schema = self.load_schema()

        if schema is None:
            print('Inferindo schema...')
            schema = self.infer_schema()
            print('Ok!')

        self.schema = schema

    def save_schema(self) -> None:
        """
        Saves the schema.

        Returns
        -------
        None
        """

        data = self.schema.jsonValue()
        schema_path = f'/tmp/datalake/eleflow/{self.source}/schema'
        self._make_dir(f'file:{schema_path}')
        schema_path_local = f'{schema_path}/{self.table_name}.json'

        with open(schema_path_local, 'w') as f:
            json.dump(data, f, indent=2)

        schema_path_dbfs = schema_path_local.replace('/tmp', 'dbfs:')
        dbutils.fs.cp(schema_path_local, schema_path_dbfs)

    def load_full(self) -> pyspark_df:
        """
        Loads the all the data from the path full load.

        Returns
        -------
        pyspark_df
            A spark dataframe.
        """

        reader = (spark
                    .read
                    .format(self.file_format)
                    .option("recursiveFileLookup","true")
                    .options(**self.read_options)
                )
        
        if self.schema is not None:
            reader = reader.schema(self.schema)

        sdf = reader.load(self.path_full_load)
        
        return sdf
    
    def save_full(self, sdf: pyspark_df) -> None:
        """
        Saves the data from the full_load path into a table.

        Parameters
        ----------
        sdf : pyspark_df
            A spark dataframe.

        Returns
        -------
        None
        """

        writer = (
            sdf.coalesce(1)
            .write.format('delta')
            .mode('overwrite')
            .option("recursiveFileLookup","true")
            .option('overwriteSchema', 'true')
        )

        if len(self.partition_fields) > 0:
            writer = writer.partitionBy(*self.partition_fields)

        writer.saveAsTable(self.table_full_name)

    def read_query(self) -> str | None:
        """
        Reads the query from the path specified.

        Returns
        -------
        str | None
            A query string to filter the data.
        """

        query_path_dbfs = f'dbfs:/datalake/eleflow/{self.source}/etl/{self.table_name}.sql'
        query_path_local = query_path_dbfs.replace('dbfs:', '/tmp')
        try:
            dbutils.fs.cp(query_path_dbfs, f'file:{query_path_local}')
        except:
            pass

        if os.path.exists(query_path_local):
            with open(query_path_local) as f:
                return f.read()
        return None
    
    def default_query(self) -> str:
        """
        Creates a default query using the primary keys and a order by clause.
        It filters duplicate values and returns only the most up to date.

        Returns
        -------
        str
            The default query.
        """

        base_query = '''SELECT *,
                        NOW() as ingestion_at
                        FROM {table} '''
        ids = ','.join(self.id_fields)
        window = f'''QUALIFY row_number() OVER (PARTITION BY {ids} ORDER BY {self.timestamp_field} DESC) = 1'''
        return base_query + window
    
    def set_query(self) -> None:
        """
        Sets the query for the table.
        It tries to load from the path, otherwise it will load the default.
        
        Returns
        -------
        None
        """

        query = self.read_query()
        if query is None:
            print('Loading default query...')
            query = self.default_query()
            print('Ok!')
        self.query = query

    def transform(self, table_name: str) -> pyspark_df:
        """
        Apply the transformation to the table.
        
        Parameters
        ----------
        table_name : str
            The name of the table.
        
        Returns
        -------
        pyspark_df
            A spark dataframe.
        """
    
        query = self.query.format(table=table_name)
        return self.spark.sql(query)

    def process_full(self) -> None:
        """
        Read and save the file contents into a table.

        Returns
        -------
        None
        
        """
        sdf = self.load_full()
        view_name_tmp = f'tb_full_{self.table_name}'
        sdf.createOrReplaceTempView(view_name_tmp)
        sdf_transform = self.transform(view_name_tmp)
        self.save_full(sdf_transform)

    def load_stream(self) -> pyspark_df:
        """
        Reads a stream.

        Returns
        -------
        pyspark_df
            A spark dataframe.
        """

        df_stream = (
            self.spark
            .readStream
            .format('cloudFiles')
            .option('cloudFiles.format', self.file_format)
            .option('cloudFiles.maxFilesPerTrigger', 10000)
            .option("recursiveFileLookup","true")
            .options(**self.read_options)
            .schema(self.schema)
            .load(self.path_incremental)
        )

        return df_stream
    
    def upsert(self, df: pyspark_df, delta_table: delta.tables.DeltaTable) -> None:
        """
        Insert the data into the delta table if it does not already exist,
        otherwise update the delta table.

        Parameters
        ----------
        df : pyspark_df
            A spark dataframe.
        delta_table : delta.tables.DeltaTable
            A delta table.
        
        Returns
        -------
        None
        """

        df.createOrReplaceGlobalTempView(f'tb_stream_{self.table_name}')

        df_new = self.transform(table_name=f'global_temp.tb_stream_{self.table_name}')

        join = ' AND '.join(f'd.{id_field} = n.{id_field}' for id_field in self.id_fields)
        (
            delta_table.alias('d')
            .merge(df_new.alias('n'), join)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    def save_stream(self, df_stream: pyspark_df) -> None:
        """
        Save the stream into the table.

        Parameters
        ----------
        df_stream : pyspark_df
            A spark dataframe.
        
        Returns
        -------
        None
        
        """
        table_delta = delta.DeltaTable.forName(self.spark, self.table_full_name)

        return (
            df_stream.writeStream.trigger(availableNow=True)
            .option('checkpointLocation', self.checkpoint_path)
            .foreachBatch(lambda df, batchID: self.upsert(df, table_delta))
        )

    def process_stream(self) -> StreamingQuery:
        """
        Process a streaming query, loading and saving the data.

        Returns
        -------
        StreamingQuery
            The streaming query.
        """
        df_stream = self.load_stream()
        stream = self.save_stream(df_stream)

        return stream.start()


# COMMAND ----------

database_name = 'bronze'
sources = ['VRA', 'AIR_CIA', 'icaos']

for source in sources:
    table_name = f'eleflow_{source}'
    
    ingestao = BronzeIngestionAirport(
        table_name=table_name,
        database_name=database_name,
        spark=spark,
        source=source
    )

    if not dbtools.table_exists(spark, database_name, table_name):
        df_null = spark.createDataFrame(data=[], schema=ingestao.schema)
        ingestao.save_full(df_null)
        dbutils.fs.rm(ingestao.checkpoint_path, True)
    
    stream = ingestao.process_stream()
    stream.awaitTermination()

    table = delta.DeltaTable.forName(spark, f"{database_name}.{table_name}")
    table.vacuum()
    
