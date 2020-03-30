from iotfunctions.db import Database
from custom import settings

import sys
import pandas as pd
import numpy as np
import json

with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default

if settings.ENTITY_NAME:
    entity_name = settings.ENTITY_NAME
elif (len(sys.argv) > 0):
    entity_name = sys.argv[1]
else:
    print("Please place ENTITY_NAME in .env file")
    exit()

print("reading table for entity_type: " + entity_name)
df = db.read_table(table_name=entity_name, schema=db_schema)
print(df.columns)
print(df.head())
