import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from custom.functions import InvokeExternalModel
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
#from iotfunctions.enginelog import EngineLogging
from custom import settings
import datetime as dt

#EngineLogging.configure_console_logging(logging.DEBUG)

'''
# Replace with a credentials dictionary or provide a credentials
# Explore > Usage > Watson IOT Platform Analytics > Copy to clipboard
# Past contents in a json file.
'''
#with open('credentials_staging.json', encoding='utf-8') as F:
with open('credentials.json', encoding='utf-8') as F:
#with open('credentials_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Developing Test Pipelines
-------------------------
When creating a set of functions you can test how they these functions will
work together by creating a test pipeline.
'''


'''
Create a database object to access Watson IOT Platform Analytics DB.
'''
db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default

'''
To do anything with IoT Platform Analytics, you will need one or more entity type.
You can create entity types through the IoT Platform or using the python API as shown below.
The database schema is only needed if you are not using the default schema. You can also rename the timestamp.
'''
# entity_name = 'kalbuildings'
entity_name = "KB_Robot_Type"

# meta = db.get_entity_type(entity_name)
# print(meta)

# db_schema = "BLUADMIN" # None  # replace if you are not using the default schema
# db.drop_table(entity_name, schema = db_schema)

# Credentials to access WML Model.
WATSON_ML_ENDPOINT = settings.WATSON_ML_ENDPOINT
WATSON_ML_UID = settings.WATSON_ML_UID
WATSON_ML_PASSWORD = settings.WATSON_ML_PASSWORD
WATSON_ML_INSTANCE_ID = settings.WATSON_ML_INSTANCE_ID
WATSON_ML_APIKEY = settings.WATSON_ML_APIKEY

entity = EntityType(entity_name, db,
                    # following columns can be dynamically generated based on meters associated with each asset
                    # Column('deviceid',String(50)),
                    # Column('evt_timestamp',String(50)),
                    InvokeExternalModel(
                                    wml_endpoint=WATSON_ML_ENDPOINT,
                                    uid=WATSON_ML_UID,
                                    password=WATSON_ML_PASSWORD,
                                    instance_id=WATSON_ML_INSTANCE_ID,
                                    apikey=WATSON_ML_APIKEY,
                                    # username="",
                                    # password="",
                                    # request="",
                                    output_item = 'http_preload_done'),
                    # bif.PythonExpression(expression='df["energy_value"]*df["energy_value"]',
                    #                      output_name = 'volume'),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema}
)


# anomaly_entity = EntityType("anomalies", db)
# anomaly_entity.register(raise_error=False)

'''
# testing creation of seperate table for anomalies
print("creating anomalies table")
db.create(tables=[{"name":"anomalies"}])
df = db.read_table(table_name="anomalies", schema=db_schema)
print("printing anomalies")
print(df.head())
print(list(df.columns))
exit()
'''

'''
When creating an EntityType object you will need to specify the name of the entity, the database
object that will contain entity data

After creating an EntityType you will need to register it so that it visible in the Add Data to Entity Function UI.
To also register the functions and constants associated with the entity type, specify
'publish_kpis' = True.
'''
entity.register(raise_error=False)
print("Entity registered")

# entity.generate_data(days=0.5, drop_existing=True)
# print("Data generated")

df = db.read_table(table_name=entity_name, schema=db_schema)
print("Table head")
print(df.head())
print("Table columns")
print(list(df.columns))


db.register_functions([InvokeExternalModel])
print("Function registered")

'''
To test the execution of kpi calculations defined for the entity type locally
use this function.

A local test will not update the server job log or write kpi data to the AS data
lake. Instead kpi data is written to the local filesystem in csv form.
'''

# entity.exec_local_pipeline()
start = dt.datetime.utcnow() - dt.timedelta(days=7)
end = dt.datetime.utcnow() - dt.timedelta(days=1)
print("Starting pipeline")
entity.exec_local_pipeline() #start_ts=start, end_ts=end)
print("Completed pipeline")

'''
view entity data
'''
# can get entity metadata with following
#
# TODO, for some reason this returns data but also throws exception.
# '''
print("getting metadata")
meta = db.get_entity_type(entity_name)
db_schema = meta['schemaName']
table_name = meta['metricTableName']
print(meta)
# '''
db_schema = "BLUADMIN"
table_name = "custom_model_data"

# df = db.read_table(table_name=table_name, schema=db_schema)
# df = db.read_table(table_name=entity_name, schema=db_schema)
# print(df.head())


print ( 'Done registering  entity %s '  %db.register_functions([InvokeExternalModel]) )
