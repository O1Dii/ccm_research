from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from set_env import set_env_from_env_file
import os


def upload_blob(filename, conn_str=None, container=None):
    set_env_from_env_file()

    if not conn_str:
        conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    if not container:
        container = os.getenv('AZURE_CONTAINER_NAME')

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(container=container, blob=filename)

    with open(filename, "rb") as data:
        blob_client.upload_blob(data)
