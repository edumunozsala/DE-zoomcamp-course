from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql import types
from pyspark.sql.types import *
from pyspark.sql import functions as F

import zipfile

import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/personal-gcp.json"

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here
    # Create the schema
    dtypes = data.to_records(index=False).tolist() 
    fields = [types.StructField(dtype[0], globals()[f'{dtype[1]}Type']()) for dtype in dtypes]
    schema = StructType(fields)
    print(schema)
    # Create the spark session, if it doesn't
    spark = (
        SparkSession.builder
        .appName('pyspark-run-with-gcp-bucket')
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
        #.config("spark.sql.repl.eagerEval.enabled", True) 
        .getOrCreate()
    )
    # Set GCS credentials if necessary
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

    # Save the spark session in the context
    kwargs['context']['spark'] = spark

    # Set the GCS location to save the data
    bucket_name="mage-dezoomcamp-ems"
    project_id="banded-pad-411315"

    table_name="events"
    source_files= f'gs://{bucket_name}/GDELT-Project/bronze/csv/*.CSV'

    root_path= f'gs://{bucket_name}/GDELT-Project/bronze/{table_name}'
    print(root_path)
    
    # Set the url where the csv data is
    #url = "https://gdelt-open-data.s3.amazonaws.com/v2/events/20240318230000.export.csv"
    #url = "https://gdelt-open-data.s3.amazonaws.com/v2/events/20240324161500.gkg.csv"
    #url="/home/src/extracted/20240324160000.export.CSV"

    #spark.sparkContext.addFile(url)
    # Read the csv data and save it into GCS in parquet format
    (spark.read
        .option("inferSchema", "true")
        .schema(schema)
        #.csv(SparkFiles.get("20240324160000.export.CSV"), schema=schema)
        .csv(source_files, sep="\t")
        #.load()
        #.select("PassengerId","Survived","Pclass","Name","Sex","Age")
        .withColumn("week", F.weekofyear(F.to_date("SQLDATE","yyyyMMdd")))
        #.select("SQLDATE","week")
        .write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("Year", "week")
        .save(root_path)
    )
    #print(df.show(3))
