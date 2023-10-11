# Databricks notebook source
#%run ../00.setup/create_files

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

import os
import json
import time
import requests
from typing import Literal, List
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as pyspark_df
from pyspark.sql import functions as F
from multiprocessing.pool import ThreadPool
import pandas as pd

# COMMAND ----------

class IngestionRawAirport:
    """
    A class to extract data from airports.

    Attributes
    ----------
    source : Literal['VRA', 'AIR_CIA']
        The source of the data.
    delimiter : str, optional
        Delimiter character used in CSV files (default is ';').
    enconding : str, optional
        Enconding used in JSON files (default is 'utf-8-sig').
    orient : str, optional
        Indication of expected JSON string format.
    output_format: Literal['csv', 'parquet', 'json']
        Format of the output file.
    folder_path : str
        The path of the folder.
    file_format : Literal['csv', 'json']
        Format of the input file.
        
    Methods
    -------
    get_file_paths() -> List[str] :
        Get a list of all files in the folder with the given file_format.
    get_df_from_csv(file_path: str, **kwargs) -> pyspark_df:
        Load the file from a CSV file to a dataframe.
    get_df_from_json(file_path: str, **kwargs) -> pyspark_df:
        Load the file from a JSON file to a dataframe.
    move_file(file_path: str) -> None:
        Moves the file when the extraction is complete.
    extract(file_path: str, **kwargs) -> pyspark_df:
        Extract the data from a file and return a dataframe.
    normalize_columns_vra(sdf: pyspark_df) -> pyspark_df:
        Changes the columns names of VRA to snake case.
    normalize_columns_air_cia(self, sdf: pyspark_df) -> pyspark_df:
        Changes the columns names of AIR CIA to snake case.
    transform(sdf: pyspark_df) -> pyspark_df:
        Apply all transformations to the dataframe.
    add_to_icaos_list(sdf: pyspark_df) -> pyspark_df:
        Add the ICAO from the dataframe to the list.
    request_icao(icao: str) -> List[dict]:
        Request from a API the information for the ICAO
    get_icaos_data(icaos: List[str]):
        Get the data for all the ICAOs and create a dataframe.
    save_file(sdf: pyspark_df, file_name: str, source: str | None, **kwargs) :
        Saves the dataframe in the specified file format.
    process_icaos(sdf: pyspark_df, vra_suffix: str) :
        Processes the infomation from the API.
    process_file(file_path: str) :
        Process a file.
    process_data() :
        Process all the data from the folder.
    """

    def __init__(self, 
                 source: Literal['VRA', 'AIR_CIA'],
                 delimiter: str = ';',
                 encoding: str = 'utf-8-sig',
                 orient: str = 'records',
                 output_format: Literal['csv', 'parquet', 'json'] = 'parquet'
                 ) -> None:
        """
        Constructs all the necessary atributes for the airport data object.

        Parameters
        ----------
        source : Literal['VRA', 'AIR_CIA']
            The source of the data.
        file_format : Literal['csv', 'json']
            Format of the input file.
        delimiter : str, optional
            Delimiter character used in CSV files (default is ';').
        enconding : str, optional
            Enconding used in JSON files (default is 'utf-8-sig').
        orient : str, optional
            Indication of expected JSON string format.
        output_format: Literal['csv', 'parquet', 'json']
            Format of the output file.
        icaos: list
            List with all the ICAOs from the VRAs files.
        """

        self.source = source
        self.delimiter = delimiter
        self.encoding = encoding
        self.orient = orient
        self.output_format = output_format
        self.icaos_list = []

        with open('/tmp/eleflow/config/01.raw/config.json') as f:
            config = json.load(f)

        self.folder_path = config[source]['folder_path']
        self.file_format = config[source]['file_format']


    def get_file_paths(self) -> List[str]:
        """
        Get a list of all files in the folder with the given file_format.

        Returns
        -------
        List[str]
            A list of strings.
        """

        folder_path = dbutils.fs.ls(f'dbfs:/{ing.folder_path}/{ing.file_format}')
        file_paths = [file_info.path for file_info in folder_path if self.file_format in file_info.path]
        if not file_paths:
            raise ValueError(f"No {self.file_format} files found in the folder {self.folder_path}")
        
        return file_paths
    

    def get_df_from_csv(self, file_path: str, **kwargs) -> pyspark_df:
        """
        Load the file from a CSV file to a dataframe.

        Parameters
        ----------
        file_path : str
            File path to the CSV file.
        kwargs : kwargs
            Keyword arguments.
            
        Returns
        -------
        pyspark_df
            A spark dataframe.
        """

        sdf = (
            spark
            .read
            .format(ing.file_format)
            .option('delimiter', self.delimiter)
            .option('header', True)
            .options(**kwargs)
            .load(file_path)
        )

        return sdf

    def get_df_from_json(self, file_path: str, **kwargs) -> pyspark_df:
        """
        Load the file from a JSON file to a dataframe.

        Parameters
        ----------
        file_path : str
            File path to the JSON file.
        kwargs : kwargs
            Keyword arguments.
            
        Returns
        -------
        pyspark_df
            A spark dataframe.
        """

        sdf = (
            spark
            .read
            .format(ing.file_format)
            .options(**kwargs)
            .load(file_path)
        )

        return sdf
    

    def move_file(self, file_path: str) -> None:
        """
        Moves the file when the extraction is complete.

        Parameters
        ----------
        file_path : str
            File path to the source file
        
        Returns
        -------
        None
        """

        new_path = file_path.replace('landing', 'proceeded')
        dbutils.fs.mv(file_path, new_path)
    

    def extract(self, file_path: str, **kwargs) -> pyspark_df:
        """
        Extract the data from a file and return a dataframe.

        Parameters
        ----------
        file_path : str
            File path to the file.
        kwargs : dict
            Keyword arguments.

        Returns
        -------
        pyspark_df
            A spark dataframe.
        """

        sdf = getattr(self, f'get_df_from_{self.file_format}')(file_path, **kwargs)
        # self.move_file(file_path)
        
        return sdf
    

    def normalize_columns_vra(self, sdf: pyspark_df) -> pyspark_df:
        """

        Changes the columns names of VRA to snake case.

        Parameters
        ----------
        sdf : pd.DataFrame
            A dataframe.

        Returns
        -------
        pyspark_df
        """

        return (sdf
                .withColumnRenamed('ICAOEmpresaAérea', 'ICAO_empresa_area')
                .withColumnRenamed('NúmeroVoo', 'numero_voo')
                .withColumnRenamed('CódigoAutorização', 'codigo_autorizacao')
                .withColumnRenamed('CódigoTipoLinha', 'codigo_tipo_linha')
                .withColumnRenamed('ICAOAeródromoOrigem', 'ICAO_aerodromo_origem')
                .withColumnRenamed('ICAOAeródromoDestino', 'ICAO_aerodromo_destino')
                .withColumnRenamed('PartidaPrevista', 'partida_prevista')
                .withColumnRenamed('PartidaReal', 'partida_real')
                .withColumnRenamed('ChegadaPrevista', 'chegada_prevista')
                .withColumnRenamed('ChegadaReal', 'chegada_real')
                .withColumnRenamed('SituaçãoVoo', 'situacao_voo')
                .withColumnRenamed('CódigoJustificativa', 'codigo_justificativa')
                )

    
    def normalize_columns_air_cia(self, sdf: pyspark_df) -> pyspark_df:
        """
        Changes the columns names of AIR CIA to snake case.

        Parameters
        ----------
        sdf : pyspark_df
            A dataframe.

        Returns
        -------
        pyspark_df
        """

        return (sdf
                .withColumnRenamed('Razão Social', 'razao_social')
                .withColumnRenamed('ICAO IATA', 'icao_iata')
                .withColumnRenamed('CNPJ', 'cnpj')
                .withColumnRenamed('Atividades Aéreas', 'atividades_aereas')
                .withColumnRenamed('Endereço Sede', 'endereco_sede')
                .withColumnRenamed('Telefone', 'telefone')
                .withColumnRenamed('E-Mail', 'email')
                .withColumnRenamed('Decisão Operacional', 'decisao_operacional')
                .withColumnRenamed('Data Decisão Operacional', 'data_decisao_operacional')
                .withColumnRenamed('Validade Operacional', 'validade_operacional')
                )
        

    def transform(self, sdf: pyspark_df) -> pyspark_df:
        """
        Apply all transformations to the dataframe.

        Parameters
        ----------
        sdf : pyspark_df
            Dataframe to be transformed.

        Returns
        -------
        pyspark_df
            Dataframe with all transformations applied.
        """

        sdf = getattr(self, f'normalize_columns_{self.source.lower()}')(sdf)
        sdf = sdf.withColumn('timestamp', F.lit(int(time.time())))

        return sdf
    

    def add_to_icaos_list(self, sdf: pyspark_df) -> None:
        """
        Add the ICAO from the dataframe to the list.

        Parameters
        ----------
        sdf : pyspark_df
            Dataframe with a column with the ICAOs.
        
        Returns
        -------
        None
        """
        
        icaos_dest = sdf.select(F.collect_set('ICAO_aerodromo_destino')).first()[0]
        icaos_orig = sdf.select(F.collect_set('ICAO_aerodromo_origem')).first()[0]
        icaos = set([*icaos_dest] + [*icaos_orig])
        self.icaos_list.extend(icaos)


    def request_icao(self, icao: str) -> dict:
        """
        Request from a API the information for the ICAO

        Parameters
        ----------
        icao : str
            Code for an airport, should be a string of 4 letters.

        Returns
        -------
        dict
            Info about the airport.
        """

        url = "https://airport-info.p.rapidapi.com/airport"

        querystring = {"icao": icao}

        headers = {
            "X-RapidAPI-Key": API_KEY,
            "X-RapidAPI-Host": "airport-info.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)
        payload = response.json()

        if not payload.get('error') and not payload.get('message'):
            return payload
        
    
    def get_icaos_data(self) -> List[dict]:
        """
        Get the data for all the ICAOs.

        Returns
        -------
        List[dict]
            A list of dicts with ICAOs info.
        """

        icaos = set(self.icaos_list)

        with ThreadPool(10) as pool:
            data = pool.map(self.request_icao, icaos)

        return data


    def save_file(self, sdf: pyspark_df, file_name: str, 
                  source: str | None = None,  **kwargs) -> None:
        """
        Saves the dataframe in the specified file format.

        Parameters
        ----------
        sdf : pyspark_df
            Dataframe to be saved as a file.
        file_name : str
            The name of the file.
        source : str |  None (optional)
            source of the data
        kwargs : kwargs
            Keyword arguments

        Returns
        -------
        None
        """

        if source == 'icaos':
            folder_path = f"{self.folder_path.replace('VRA', 'icaos').replace('landing', 'raw')}/{self.output_format}"
        else:
            folder_path = f"{self.folder_path.replace('landing', 'raw')}/{self.output_format}"
            
        filepath = f'{folder_path}/{file_name}.{self.output_format}'

        if self.output_format == 'json' and not kwargs:
            kwargs = {'orient': 'records', 'indent': 4}
            
        (
            sdf
            .write
            .format(self.output_format)
            .mode('overwrite')
            .option("overwriteSchema", "true")
            .options(**kwargs)
            .save(filepath)
        )
    

    def process_icaos(self) -> None:
        """
        Processes the infomation from the API.

        Returns
        -------
        None
        """

        raw_data = self.get_icaos_data()
        data = [i for i in raw_data if pd.notna(i)]
        df_icaos = pd.DataFrame(data)
        timestamp = int(time.time())
        df_icaos['timestamp'] = timestamp
        sdf_icaos = spark.createDataFrame(df_icaos)
        self.save_file(sdf_icaos, file_name=f'icaos_info_{timestamp}', source='icaos')


    def process_file(self, file_path: str) -> None:
        """
        Process a file.

        Parameters
        ----------
        file_path : str 
            The path to the folder.
        
        Returns
        -------
        None
        """
        
        sdf = self.extract(file_path)
        sdf = self.transform(sdf)

        file_name = file_path.split('/')[-1].split('.')[0]

        if self.source == 'VRA':
            self.add_to_icaos_list(sdf)

        self.save_file(sdf, file_name)


    def process_data(self) -> None:
        """
        Process all the data from the folder.
        """
        
        file_paths = self.get_file_paths()
        
        with ThreadPool(10) as pool:
            pool.map(self.process_file, file_paths)

        self.process_icaos()


# COMMAND ----------

sources = ['VRA', 'AIR_CIA']

for source in sources:
    ing = IngestionRawAirport(
        source=source
    )

    try: 
        nb_bf_running = dbutils.fs.ls(f"dbfs:/{ing.folder_path}/{ing.output_format}")
    except Exception as e:
        nb_bf_running = []
    print(f'Number of files for {ing.source} source before script: {len(nb_bf_running)}')

    ing.process_data()

    try: 
        nb_af_running = dbutils.fs.ls(f"dbfs:/eleflow/datalake/raw/{ing.source}/{ing.output_format}")
    except Exception as e:
        nb_af_running = []
    print(f'Number of files for {ing.source} source after script: {len(nb_af_running)}')    

# COMMAND ----------

ing = IngestionRawAirport(
    source='AIR_CIA'
)
fps = ing.get_file_paths()

# COMMAND ----------

sdf = ing.extract(fps[0])
sdf = ing.transform(sdf)


# COMMAND ----------

sdf.display()

# COMMAND ----------

file_name = file_path.split('/')[-1].split('.')[0]
ing.save_file(sdf, file_name)

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/eleflow/datalake/raw/icaos/{ing.output_format}")

# COMMAND ----------

sdf_icaos = (spark
.read
.format('parquet')
.option("recursiveFileLookup","true")
.load('dbfs:/eleflow/datalake/raw/icaos')
)