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
#EngineLogging.configure_console_logging(logging.DEBUG)

'''
# Replace with a credentials dictionary or provide a credentials
# Explore > Usage > Watson IOT Platform Analytics > Copy to clipboard
# Past contents in a json file.
'''
with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Developing Test Pipelines
-------------------------
When creating a set of functions you can test how they these functions will
work together by creating a test pipeline.
'''


# t = BaseTransformer()
'''
Create a database object to access Watson IOT Platform Analytics DB.
'''
db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default

entity_name = 'kb_anomaly'
entity = EntityType(entity_name, db,
                    Column('deviceid',String(50)),
                    Column('anomaly_score', Integer()),
                    Column("torque", Integer()),
                    Column("acc", Integer()),
                    Column("load", Integer()),
                    Column("tool_type", Integer()),
                    Column("speed", Float()),
                    Column("travel_time", Float()),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      # '_production_mode': False,
                      '_db_schema' : db_schema}
)

# db.unregister_functions(["InvokeExternalModel"])
# exit()
db.register_functions([InvokeModel])
# exit()
print("Function registered")

entity.register(raise_error=False)
print("Entity registered")

# generate data and set anomaly_score to zeros
entity.generate_data(days=2.0, drop_existing=True)
df = db.read_table(table_name=entity_name, schema=db_schema)
df["anomaly_score"] = np.zeros(len(df))
db.write_frame(df, table_name=entity_name, schema=db_schema, if_exists="replace")
db.commit()
