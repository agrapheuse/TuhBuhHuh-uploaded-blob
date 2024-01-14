import azure.functions as func
import logging
import os
import json
import pandas as pd
import uuid
from abc import ABC, abstractmethod
from azure.storage.blob import BlobServiceClient
from datetime import datetime

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="message", queue_name="api-data-to-etl",
                               connection="AzureWebJobsServiceBus") 
def uploadedBlob(message: func.ServiceBusMessage):
    folder_name = message.get_body().decode('utf-8')
    logging.warning(f"Python ServiceBus queue trigger processed message: {folder_name}")
    connection_string = "DefaultEndpointsProtocol=https;AccountName=datalaketuhbehhuh;AccountKey=C2te9RgBRHhIH8u3tydAsn9wNd4umdD2axq1ZdcfKh7CZRpL04+D4H6QinE/gckMTUA/dFj1kFpd+ASt4+/8ZA==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    content = download_blob_to_file(blob_service_client, "json", folder_name)

    folder_name = folder_name.split("/")[0]

    logging.warning(f"Converting {folder_name} data")
    api, df = convert(blob_service_client, content, folder_name)
    if df.empty:
        return
    else:
        upload_blob(blob_service_client, api, df)

def download_blob_to_file(blob_service_client: BlobServiceClient, container_name, blob_name):
    logging.warning(f"Downloading blob: {blob_name} from container: {container_name}")
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob()
    logging.warning(f"Succesfully downloaded blob: {blob_name}")

    logging.warning(f"Converting blob to string")
    json_content = blob_data.readall().decode('utf-8')
    return json_content


def convert(blob_service_client, content, folder_name):
    converter_class = converter_mapping.get(folder_name)

    logging.warning('getting grid')
    grid = download_blob_to_file(blob_service_client, "grid", "grid.csv")

    if converter_class:
        logging.warning(f'Converter found: {converter_class}')
        converter_instance = converter_class()
        json_data = json.loads(content)
        folder_name, df = converter_instance.convert(json_data, grid)
        if folder_name == "-1":
            logging.warning(f'No csv file created for blob name: {folder_name}')
        return folder_name, df
    else:
        logging.warning(f'No converter found for blob name: {folder_name}')

class Converter:
    @abstractmethod
    def convert(self, json_dict):
        pass

class TelRaam(Converter, ABC):
    def convert(self, json_dict, grid):
        logging.warning(f"Converting TelRaam data")
        grid_config_path = 'grid.config.json'

        with open(grid_config_path, 'r') as json_file:
            grid = json.load(json_file)
            zone_def = grid.get('zoneDef')
            gridEndLat = float(zone_def.get("gridEndLat"))
            gridStartLat = float(zone_def.get("gridStartLat"))
            gridEndLong = float(zone_def.get("gridEndLong"))
            gridStartLong = float(zone_def.get("gridStartLong"))

        statusCode = json_dict["statusCode"]
        if statusCode != 200:
            return -1, pd.DataFrame()
        
        timeSent = json_dict["timeSent"].replace(":", "-")
        try:
            body = json.loads(json_dict["body"])
        except TypeError:
            body = json_dict["body"]
        features = body["features"]

        keptFeatures = []
        for f in features:
            inside = True
            for c in f["geometry"]["coordinates"][0]:
                if c[0] < gridStartLong or c[0] > gridEndLong or c[1] > gridStartLat or c[1] < gridEndLat:
                    inside = False
            if inside:
                keptFeatures.append(f)

        finalFeatures = []
        for f in keptFeatures:
            if f["properties"]["uptime"] != '':
                finalFeatures.append(f)
        
        df = pd.DataFrame(columns=['latitude', 'longitude', 'timeStamp', 'heavy', 'car', 'bike', 'pedestrian', 'v85'])
        for f in finalFeatures:
            coordinatesStart = f["geometry"]["coordinates"][0][0]
            coordinatesEnd = f["geometry"]["coordinates"][0][-1]
            latitude = (coordinatesStart[1] + coordinatesEnd[1]) / 2
            longitude = (coordinatesStart[0] + coordinatesEnd[0]) / 2
            try:
                timeStamp = datetime.strptime(f["properties"]["last_data_package"], "%Y-%m-%d %H:%M:%S.%f%z").strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                timeStamp = datetime.strptime(f["properties"]["last_data_package"], "%Y-%m-%d %H:%M:%S+00:00").strftime("%Y-%m-%d %H:%M:%S")
            heavy = f["properties"]["heavy"]
            car = f["properties"]["car"]
            bike = f["properties"]["bike"]
            pedestrian = f["properties"]["pedestrian"]
            v85 = f["properties"]["v85"]
            df.loc[len(df)] = [latitude, longitude, timeStamp, heavy, car, bike, pedestrian, v85]
        
        final_df = pd.DataFrame(columns=['uuid', 'timestamp', 'latitude', 'longitude', 'valueType', 'sensorDataValue'])
        for index, row in df.iterrows():
            final_df.loc[len(final_df)] = [uuid.uuid4(), row["timeStamp"], row["latitude"], row["longitude"], "HEAVY", row["heavy"]]
            final_df.loc[len(final_df)] = [uuid.uuid4(), row["timeStamp"], row["latitude"], row["longitude"], "CAR", row["car"]]
            final_df.loc[len(final_df)] = [uuid.uuid4(), row["timeStamp"], row["latitude"], row["longitude"], "BIKE", row["bike"]]
            final_df.loc[len(final_df)] = [uuid.uuid4(), row["timeStamp"], row["latitude"], row["longitude"], "PEDESTRIAN", row["pedestrian"]]
            final_df.loc[len(final_df)] = [uuid.uuid4(), row["timeStamp"], row["latitude"], row["longitude"], "V85", row["v85"]]

        logging.warning(f"Succesfully converted TelRaam data")
        logging.warning(f"Added {len(df)} rows to the dataframe")
        return "TelRaam", final_df

class OpenSenseMap(Converter, ABC):
    def convert(self, json_dict, grid):

        grid_config_path = 'grid.config.json'

        with open(grid_config_path, 'r') as json_file:
            grid = json.load(json_file)
            zone_def = grid.get('zoneDef')
            gridEndLat = float(zone_def.get("gridEndLat"))
            gridStartLat = float(zone_def.get("gridStartLat"))
            gridEndLong = float(zone_def.get("gridEndLong"))
            gridStartLong = float(zone_def.get("gridStartLong"))

        '''
        uuid = json_dict["uuid"]
        statusCode = json_dict["statusCode"]
        if statusCode != 200:
            return
        
        timeSent = json_dict["timeSent"].replace(":", "-")
        body = json.loads(json_dict["body"])
        print(uuid, timeSent, statusCode, body)
        '''
        return "OpenSenseMap", pd.DataFrame()
    
class SensorCommunity(Converter, ABC):
    def convert(self, json_dict, grid):
        logging.warning(f"Converting SensorCommunity data")
        grid_config_path = 'grid.config.json'

        with open(grid_config_path, 'r') as json_file:
            grid = json.load(json_file)
            zone_def = grid.get('zoneDef')
            gridEndLat = float(zone_def.get("gridEndLat"))
            gridStartLat = float(zone_def.get("gridStartLat"))
            gridEndLong = float(zone_def.get("gridEndLong"))
            gridStartLong = float(zone_def.get("gridStartLong"))

        statusCode = json_dict["statusCode"]
        if statusCode != 200:
            return -1, pd.DataFrame()
        
        try:
            body = json.loads(json_dict["body"])
        except TypeError:
            body = json_dict["body"]

        keptSensor = []
        for sensordata in body:
            lat = float(sensordata['location']["latitude"])
            lon = float(sensordata['location']["longitude"])
            if lon > gridStartLong and lon < gridEndLong and lat < gridStartLat and lat > gridEndLat:
                if sensordata["location"]['indoor'] == 0:
                    keptSensor.append(sensordata)
            
        df = pd.DataFrame(columns=['uuid', 'timestamp', 'latitude', 'longitude', 'valueType', 'sensorDataValue'])
        for sensordata in keptSensor:
            lat = float(sensordata['location']["latitude"])
            lon = float(sensordata['location']["longitude"])
            timeStamp = sensordata['timestamp']
            for values in sensordata["sensordatavalues"]:
                value_type = values["value_type"]
                value = values["value"]
                if value_type in ["temperature", "humidity", "P1", "P2"]:
                    df.loc[len(df)] = [uuid.uuid4(), timeStamp, lat, lon, value_type.upper(), value]
        logging.warning(f"Succesfully converted SensorCommunity data")
        logging.warning(f"Added {len(df)} rows to the dataframe")
        return "SensorCommunity", df

converter_mapping = {
    '8c9a8f25-e54e-4884-aee6-a4529c5424ba': TelRaam,
    '2889936e-8e2d-11ee-b9d1-0242ac120002': OpenSenseMap,
    '017f12f5-8acb-4531-ab77-0e5208a31bca': SensorCommunity
}

def upload_blob(blob_service_client, api, df):  
    blob_uuid = uuid.uuid4()
    logging.warning(f"Uploading {api}-{blob_uuid}.csv to blob storage")
    csv_string = df.to_csv(index=False)
    blob_client = blob_service_client.get_blob_client(container="csv/latest", blob=f"{api}-{blob_uuid}.csv")
    
    blob_client.upload_blob(csv_string, blob_type="BlockBlob")
    logging.warning(f"Succesfully uploaded {api}-{blob_uuid}.csv to blob storage")
