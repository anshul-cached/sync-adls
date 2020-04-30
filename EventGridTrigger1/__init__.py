import json
import logging
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from azure.identity import ClientSecretCredential

import azure.functions as func


def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })
    
    results=json.loads(result)["data"]
    client_id="XXXXXX"
    tenant_id="XXXXXX"
    client_secret="XXXXX"
    storage_account_name_destination="XXXXXXX"
    logging.info('Python EventGrid trigger processed an event: %s', results)
    sync_data(client_id,client_secret,tenant_id,storage_account_name_destination,[results])
    

    logging.info('Python EventGrid trigger processed an event: %s', result)


def sync_data(client_id,client_secret,tenant_id,storage_account_name_destination,results):
    
    for result in results:
        print(result)
        fs_credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        logging.info('Python EventGrid trigger processed an event: %s', result)
        # storage_account_name=result["url"].split(".",1)[0].replace("https://","")
        # storage_account_name=result["topic"].rsplit("/",1)[1]
        if result["api"]=="RenameFile":
            storage_account_name=result["destinationUrl"].split(".",1)[0].replace("https://","")
            url_split=result["destinationUrl"].split("/",4)
        else:
            storage_account_name=result["url"].split(".",1)[0].replace("https://","")
            url_split=result["url"].split("/",4)
        
        fs_name=url_split[3]
        blob_path=url_split[4]
        service_client=DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=fs_credential)
        fs_system_client=service_client.get_file_system_client(file_system=fs_name)
        file_client=fs_system_client.get_file_client(blob_path)
        download_data=file_client.download_file().readall()
        service_client_destination=DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name_destination), credential=fs_credential)
        fs_list=[i.name for i in service_client_destination.list_file_systems()]
        if fs_name not in fs_list:
            service_client_destination.create_file_system(fs_name)
        storage_account_fs_name=fs_name
        fs_system_client_destination=service_client_destination.get_file_system_client(file_system=fs_name)
        fc_destination=fs_system_client_destination.create_file(blob_path)
        fc_destination.upload_data(download_data,overwrite=True)
        


