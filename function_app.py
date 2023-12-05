import azure.functions as func
import logging
import os
import json
import pandas as pd
import uuid
from abc import ABC, abstractmethod
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

@app.function_name(name="uploadedBlob")
@app.blob_trigger(arg_name="obj", path="json/{name}",
                               connection="MyStorageAccountConnection") 
def uploadedBlob(obj: func.InputStream):
    content = obj.read().decode('utf-8')
    folder_name = os.path.basename(os.path.dirname(obj.name))
    
    api, df = convert(content, folder_name)
    if df.empty:
        return
    else:
        upload_blob(api, df)

def convert(content, folder_name):
    converter_class = converter_mapping.get(folder_name)

    if converter_class:
        converter_instance = converter_class()
        json_data = json.loads(content)
        folder_name, df = converter_instance.convert(json_data)
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
    def convert(self, json_dict):
        grid_config_path = 'grid.config.json'

        with open(grid_config_path, 'r') as json_file:
            grid = json.load(json_file)
            zone_def = grid.get('zoneDef')
            gridEndLat = float(zone_def.get("gridEndLat"))
            gridStartLat = float(zone_def.get("gridStartLat"))
            gridEndLong = float(zone_def.get("gridEndLong"))
            gridStartLong = float(zone_def.get("gridStartLong"))

        uuid = json_dict["uuid"]
        statusCode = json_dict["statusCode"]
        if statusCode != 200:
            return -1, pd.DataFrame()
        
        timeSent = json_dict["timeSent"].replace(":", "-")
        body = json.loads(json_dict["body"])
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
        
        df = pd.DataFrame(columns=['latitude', 'longitude', 'period', 'uptime', 'heavy', 'car', 'bike', 'pedestrian', 'v85'])
        for f in finalFeatures:
            coordinatesStart = f["geometry"]["coordinates"][0][0]
            coordinatesEnd = f["geometry"]["coordinates"][0][-1]
            latitude = coordinatesStart
            period = f["properties"]["period"]
            uptime = f["properties"]["uptime"]
            heavy = f["properties"]["heavy"]
            car = f["properties"]["car"]
            bike = f["properties"]["bike"]
            pedestrian = f["properties"]["pedestrian"]
            v85 = f["properties"]["v85"]
            df.loc[len(df)] = [coordinatesStart, coordinatesEnd, period, uptime, heavy, car, bike, pedestrian, v85]
        return "TelRaam", df

class OpenSenseMap(Converter, ABC):
    def convert(self, json_dict):
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
        print(json_dict)
        return "OpenSenseMap", pd.DataFrame()
    
class SensorCommunity(Converter, ABC):
    def convert(self, json_dict):
        
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
        body = json.loads(json_dict["body"])

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
        return "SensorCommunity", df

converter_mapping = {
    '8c9a8f25-e54e-4884-aee6-a4529c5424ba': TelRaam,
    '2889936e-8e2d-11ee-b9d1-0242ac120002': OpenSenseMap,
    '017f12f5-8acb-4531-ab77-0e5208a31bca': SensorCommunity
}

def upload_blob(api, df):
    connection_string = os.environ["MyStorageAccountConnection"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    csv_string = df.to_csv(index=False)
    blob_client = blob_service_client.get_blob_client(container="csv/latest", blob=f"{api}.csv")
    
    blob_client.upload_blob(csv_string, blob_type="BlockBlob")
    logging.info(f"uploaded {api}.csv to blob storage")
