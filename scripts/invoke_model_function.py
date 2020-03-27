import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from custom.functions import InvokeModel
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.base import BaseTransformer
from iotfunctions.bif import EntityDataGenerator
#from iotfunctions.enginelog import EngineLogging
from custom import settings
import datetime as dt

import pandas as pd
import numpy as np

with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Create a database object to access Watson IOT Platform Analytics DB.
'''
db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default

# Credentials to access WML Model.
WATSON_ML_ENDPOINT = settings.WATSON_ML_ENDPOINT
WATSON_ML_INSTANCE_ID = settings.WATSON_ML_INSTANCE_ID
WATSON_ML_APIKEY = settings.WATSON_ML_APIKEY
WATSON_ML_DEPLOYMENT_ID = settings.WATSON_ML_DEPLOYMENT_ID

IAM_UID = settings.IAM_UID
IAM_PASSWORD = settings.IAM_PASSWORD

MODEL_INPUT_COLUMNS = settings.MODEL_INPUT_COLUMNS or []
if MODEL_INPUT_COLUMNS and (len(MODEL_INPUT_COLUMNS) > 0):
    MODEL_INPUT_COLUMNS = MODEL_INPUT_COLUMNS.replace(' ', '').split(',')
else:
    MODEL_INPUT_COLUMNS = []

if settings.ENTITYNAME:
    entity_name = settings.ENTITYNAME
else:
    print("Please place ENTITY_NAME in .env file")
    exit()

entity = EntityType(entity_name, db,
                    # following columns can be dynamically generated based on meters associated with each asset
                    Column('deviceid' ,String(50)),
                    # Column('evt_timestamp',String(50)),
                    Column('anomaly_score', Integer()),
                    Column("torque", Integer()),
                    Column("acc", Integer()),
                    Column("load", Integer()),
                    Column("tool_type", Integer()),
                    Column("speed", Float()),
                    Column("travel_time", Float()),
                    InvokeModel(
                                    wml_endpoint=WATSON_ML_ENDPOINT,
                                    # model_id=WATSON_ML_MODEL_ID,
                                    deployment_id=WATSON_ML_DEPLOYMENT_ID,
                                    apikey=WATSON_ML_APIKEY,
                                    input_features=MODEL_INPUT_COLUMNS,
                                    output_item = 'anomaly_score_done'),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema}
)
entity.exec_local_pipeline()

# job_settings = {'_production_mode': False}
# entity.exec_local_pipeline(**job_settings)
