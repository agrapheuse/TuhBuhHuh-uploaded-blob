# What does this function app do ?

This script is part of an Azure Function App that triggers when a new blob (in this case, a JSON file) is added to the Azure Storage account.

## First Step: ETL
It will map the JSON file to the right converter using a dictionary. Then, it will load the JSON data and convert it using the right converter class.

### Converting dynamically
It goes through the data of the json and for each sensor data, it will dynamically create a Sensor class that has a variable for all the attributes given in the JSON data. It then puts all the data in a pandas dataframe, converts it to csv and stores it in the "csv/" folder

## Second Step: Upload CSV back into the storage
First it creates a client that interacts with our storage account. Then it goes through all of the csv files that were just created and does teh following:
- It creates a client that can interact with our blob in the "csv" container of our storage.
- It gets all the contents of the csv that we want to upload
- It uploads the blob with the contents of the csv

## Third Step: Send data to business app
To send the data that we just transformed and cleaned to the business app, we use RabbitMQ.
First it defines the parameters of the connection, then it creates a connection to the RabbitMQ server using pika and creates a queue called "data_queue"
Afterwards, it goes through all the csv files and publishes them on the queue

# How can you add an API endpoint ?
## Step 1: Create your converter class
Create a python class that looks like this:  
`class MyConverter(Converter, ABC)`  
This class needs to contain an implementation of the function `def convert(self, json_dict)`


## Step 2: Format the data
Format the data in a way that you have a list of all the JSON keys for each sensor. Then create the Sensor class dynamically using this:  
``New_sensor = type(type_name, (Sensor,), {})``  
``Object = New_sensor(**sensor)``  
``csv_file_name = Object.to_csv(type_name)``  
This will write the CSV file to the "csv/" folder

## Step 3: Add your converter to the converter mapping
In the ``converter_mapping.py``, add your new converter:
- As a key, add the name of your JSON file
- As the value, add the name of teh converter class you just created 

