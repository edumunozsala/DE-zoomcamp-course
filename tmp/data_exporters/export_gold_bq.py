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
    # Save the spark session in the context
    # Set the GCS location to save the data
    bucket_name=kwargs['bucket_name']
    project_id=kwargs['project_id']

    table_name=kwargs['table_name']
    source_root_path= f'gs://{bucket_name}/{kwargs["path"]}/{table_name}'
    print(source_root_path)
    
    dataset_name = kwargs['dataset']
    temp_bucket_name = "gdelttesttempbucket"

    # Set the url where the csv data is
    #url = "https://gdelt-open-data.s3.amazonaws.com/v2/events/20240318230000.export.csv"
    #url = "https://gdelt-open-data.s3.amazonaws.com/v2/events/20240324161500.gkg.csv"
    #url="/home/src/extracted/20240324160000.export.CSV"

    #spark.sparkContext.addFile(url)
    # Read the csv data and save it into GCS in parquet format
    (
        kwargs['context']['spark'].read
        .format("parquet")
        .load(source_root_path)
        .write
        .format('bigquery')
        .option('parentProject', project_id)
        .option("temporaryGcsBucket", temp_bucket_name)
        .mode("overwrite")
        .option("partitionField", "week")
        .option("partitionRangeStart", data['min'][0])
        .option("partitionRangeEnd", data['max'][0])
        .option("partitionRangeInterval", 1)
        .save(f"{project_id}.{dataset_name}.{table_name}")
    )


