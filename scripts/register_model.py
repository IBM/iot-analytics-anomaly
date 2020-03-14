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

from watson_machine_learning_client import WatsonMachineLearningAPIClient
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
import sklearn
with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Create a database object to access Watson IOT Platform Analytics DB.
'''
db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default



with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default
entity_name = 'kb_anomaly'

def register_custom_model_wml(df, columns=[]):
    # initialize WML client
    wml_creds_path = 'wml_credentials.json'
    wml_credentials = json.loads(open(wml_creds_path).read())
    client = WatsonMachineLearningAPIClient( wml_credentials )
    # create a model (using IsolationForest here with default settings)
    clf = IsolationForest()
    # place classifier in a sklearn "pipeline"
    pipeline = Pipeline( steps = [ ( "classifier", clf ) ] )
    # clean data, reduce to relevant input columns
    s_df = df[columns]
    # create list of df rows
    rows = [list(r) for i,r in s_df.iterrows()]
    # fit df rows in pipeline
    pipeline.fit(rows)
    # pipeline.predict(rows)
    # publish pipeline to WML
    # client.runtimes.store({
    #     client.runtimes.ConfigurationMetaNames.NAME: "test",
    #     client.runtimes.ConfigurationMetaNames.PLATFORM: {"name": "python", "version": "3.5"}
    # })
    sk_version = sklearn.__version__
    metadata = {
        client.repository.ModelMetaNames.FRAMEWORK_VERSION: 'scikit-learn',
        client.repository.ModelMetaNames.FRAMEWORK_VERSION: sk_version,
        client.repository.ModelMetaNames.NAME: 'anomaly_model',
        client.repository.ModelMetaNames.RUNTIME_NAME: 'python',
        client.repository.ModelMetaNames.RUNTIME_VERSION: '3.5'
    }
    model_details_inmem = client.repository.store_model( pipeline, meta_props=metadata)
    model_id_inmem = model_details_inmem["metadata"]["guid"]
    print("got model_id")
    deployment_details_inmem = client.deployments.create( artifact_uid=model_id_inmem, name="anomaly_model" )
    deployment_id = deployment_details_inmem["metadata"]["guid"]
    # Test model
    # data_to_post = {"values": rows}
    # model_endpoint_url_inmem = client.deployments.get_scoring_url( deployment_details_inmem )
    # client.deployments.score( model_endpoint_url_inmem, data_to_post )
    print("Place model id and deployment in .env file")
    print("model_id: " + model_id)
    print("deployment_id: " + deployment_id)
    return (model_id, deployment_id)

print("loading entity data")
df = db.read_table(table_name=entity_name, schema=db_schema)
print("entity data loaded")
columns = ['torque', 'acc', 'load', 'speed', 'tool_type', 'travel_time']
model_id, deployment_id = register_custom_model_wml(df, columns)


'''
def register_custom_model_srom(df):
    # directions found here https://github.ibm.com/srom/workshop-docs/blob/master/notebooks/auto_regression/auto_regression.ipynb
    url = "https://github.ibm.com/srom/srom/blob/master/srom/anomaly_detection/generalized_anomaly_model.py"
    filename = wget.download('https://github.com/IBMDataScience/sample-notebooks/raw/master/Files/sklearn_arima-0.1.zip')
    # register in SROM #
    from srom.wml.pipeline_executor import WMLExecutor
    wml_exec = WMLExecutor()
    wml_exec.add_pip_package(package_name='srom',
                             version=SROM_VERSION,
                             extra_index_url=SROM_PYPY_REPO,
                             pip_access_key=SROM_INSTALL_KEY)
    wml_exec.connect(cos_credentials=cos, wml_credentials=wml)
    runtime_uid = wml_exec.create_srom_runtime(runtime_name="srom-"+str(SROM_VERSION))
    scoring_endpoint = wml_exec.deploy_pipeline(model_to_deploy=am, deployment_name="anomaly_model")
'''
