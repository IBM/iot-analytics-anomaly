import json
import logging
import sys
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from custom.functions import InvokeWMLModel
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

INPUT_ITEMS = settings.INPUT_ITEMS or []
if INPUT_ITEMS and (len(INPUT_ITEMS) > 0):
    INPUT_ITEMS = INPUT_ITEMS.replace(' ', '').split(',')
else:
    INPUT_ITEMS = []

if settings.ENTITY_NAME:
    entity_name = settings.ENTITY_NAME
elif (len(sys.argv) > 0):
    entity_name = sys.argv[1]
else:
    print("Please place ENTITY_NAME in .env file")
    exit()


entity = EntityType(entity_name, db,
                    InvokeWMLModel(
                                    wml_endpoint=WATSON_ML_ENDPOINT,
                                    instance_id=WATSON_ML_INSTANCE_ID,
                                    deployment_id=WATSON_ML_DEPLOYMENT_ID,
                                    apikey=WATSON_ML_APIKEY,
                                    input_items=INPUT_ITEMS,
                                    output_items = 'anomaly_score_done'),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema}
)
entity.exec_local_pipeline()

# job_settings = {'_production_mode': False}
# entity.exec_local_pipeline(**job_settings)
