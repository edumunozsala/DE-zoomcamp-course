import requests
import zipfile
from io import BytesIO
from google.cloud import storage
from google.oauth2 import service_account
import os
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/personal-gcp.json"

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def download_and_upload_to_gcs(url, path, bucket, client):
    # Download the zip file from the URL
    response = requests.get(url)
    
    if response.status_code == 200:
        # Extract the contents of the zip file
        with zipfile.ZipFile(BytesIO(response.content), 'r') as zip_ref:
            file_list = zip_ref.namelist()
            for file_name in file_list:
                with zip_ref.open(file_name) as file:
                    # Upload extracted files to Google Cloud Storage
                    upload_to_gcs(file, path+file_name, bucket, client)

def upload_to_gcs(file_content, file_name, bucket, client):
    blob = storage.Blob(file_name, bucket)
    blob.upload_from_file(file_content, content_type='application/csv')

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    # Set the GCS location to save the data
    # Read the list of files in the project
    url_file_list='http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
    df = pd.read_csv(url_file_list, sep=' ', header=None)
    df.columns = ['ID', 'md5sum', 'file_name']
    # REmove some rows with null values
    df = df.dropna()
    # Select the files in 2024-03-01
    month_to_download='202403'
    df_files = df[(df['file_name'].str.contains(month_to_download) & (df['file_name'].str.contains('export')))]
    print("Files to download: ",len(df_files))
    
    zip_urls = df_files['file_name'].values.tolist()

    bucket_name="mage-dezoomcamp-ems"
    project_id="banded-pad-411315"

    path="GDELT-Project/bronze/csv/"
    root_path= f'gs://{bucket_name}/{path}'
    print(root_path)

    credentials = service_account.Credentials \
        .from_service_account_file(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

    client = storage.Client(
        project=project_id,
        credentials=credentials
    )

    bucket = client.bucket(bucket_name)
    #url="http://data.gdeltproject.org/gdeltv2/20240324161500.export.CSV.zip"
    for url in zip_urls:
        download_and_upload_to_gcs(url, path, bucket, client)

    return {}
