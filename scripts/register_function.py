import json
import logging
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

import sys
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

function_name = "InvokeWMLModel"
db.unregister_functions(function_name)
db.register_functions([InvokeWMLModel])
print("Function registered")
exit()
