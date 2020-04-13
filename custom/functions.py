import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
import numpy as np
import pandas as pd
import json
import base64
import requests

#from iotfunctions.base import BaseTransformer
from iotfunctions.base import BasePreload
from iotfunctions.base import BaseTransformer
from iotfunctions import ui
from iotfunctions.db import Database
from iotfunctions import bif
#import datetime as dt
import datetime
import urllib3
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


# Specify the URL to your package here.
# This URL must be accessible via pip install
PACKAGE_URL = 'git+https://github.com/IBM/iot-analytics-anomaly@'


class InvokeWMLModel(BaseTransformer):
    # _allow_empty_df = True  # allow this task to run even if it receives no incoming data
    # produces_output_items = False  # this task does not contribute new data items
    # requires_input_items = True  # this task does not require dependent data items


    # def __init__(self, ):
    def __init__(self, wml_endpoint, instance_id, deployment_id, apikey, input_items, output_items = 'http_preload_done'):
        logging.debug("in init function")
        super().__init__()
        self.input_items = input_items
        self.output_items = output_items
        self._output_list = [output_items]
        logging.debug('output_items %s', output_items)
        logging.debug('input_items %s' , input_items)
        input_items.sort()
        logging.debug('sorted input_items %s' , input_items)
        self.input_columns = input_items #.replace(' ', '').split(',')
        self.wml_endpoint = wml_endpoint
        # https://dataplatform.cloud.ibm.com/docs/content/wsj/analyze-data/ml-authentication.html
        self.uid = "bx"
        self.password = "bx"
        self.instance_id = instance_id
        self.deployment_id = deployment_id
        self.apikey = apikey
        logging.debug("finished init")


    def invoke_model(self, df, wml_endpoint, uid, password, instance_id, deployment_id, apikey, input_columns=[]):
        # Taken from https://github.ibm.com/Shuxin-Lin/anomaly-detection/blob/master/Invoke-WML-Scoring.ipynb
        # Get an IAM token from IBM Cloud
        logging.debug("posting enitity data to WML model")
        url     = "https://iam.bluemix.net/oidc/token"
        headers = { "Content-Type" : "application/x-www-form-urlencoded" }
        data    = "apikey=" + apikey + "&grant_type=urn:ibm:params:oauth:grant-type:apikey"
        response  = requests.post( url, headers=headers, data=data, auth=( uid, password ) )
        if 200 != response.status_code:
            logging.error('error getting IAM token')
            logging.error( response.status_code )
            logging.error( response.reason )
            return []
        else:
            logging.debug('token successfully generated')
            iam_token = response.json()["access_token"]
            # Send data to deployed model for processing
            headers = { "Content-Type" : "application/json",
                        "Authorization" : "Bearer " + iam_token,
                        "ML-Instance-ID" : instance_id }
            logging.debug("posting to WML")
            # TODO
            #input_columns = ['torque', 'acc', 'load', 'speed', 'tool_type', 'travel_time']
            # input_columns = ['drvn_flow', 'drvn_t1', 'drvn_t2', 'drvn_p1', 'drvn_p2']
            if (len(input_columns) == 1):
                logging.debug("filtering columns")
                logging.debug(self.input_columns)
                s_df = df[input_columns]
                items = [[i] for r,i in s_df.iteritems() ]
                # rows = [[i] for r,i in df['deviceid'].iteritems() ]
                payload = {"values": items}
            elif (len(input_columns) > 1):
                s_df = df[input_columns]
                rows = [list(r) for i,r in s_df.iterrows()]
                payload = {"values": rows}
            else:
                logging.debug("no input columns provided, forwarding all")
                s_df = df

            wml_model_endpoint = '%s/v3/wml_instances/%s/deployments/%s/online' %(wml_endpoint, instance_id, deployment_id)
            r = requests.post( wml_model_endpoint, json=payload, headers=headers )
            # should return json containing same number of predictions
            logging.debug('model response code: ' + str(r.status_code) )
            if r.status_code == 200:
                logging.debug('model response')
                logging.debug(r.text)
                j = r.json()
                logging.debug('json')
                logging.debug(j)
                return j
            else:
                logging.error('error invoking model')
                logging.error(r.status_code)
                logging.error(r.text)
                return None

    def execute(self, df): # , force_overwrite=True, start_ts = None,end_ts=None):
        # BaseTransformer()
        # TODO, set time range if not provided. Grab all rows within x hours
        logging.debug('in execution method')
        logging.debug('df.columns %s', df.columns)
        logging.debug('self.input_items %s', self.input_items)
        logging.debug('self.output_items %s', self.output_items)
        logging.debug('processing %s rows', len(df))
        df = df.copy()

        window_size = 100

        # unscored_rows_idx = df.loc[df['anomaly_score'] == 0, :].index
        # unscored_rows = df.iloc[unscored_rows_idx]
        # TODO, add logic to only send rows that don't have any score yet
        # results = self.invoke_model(unscored_rows, self.wml_endpoint, self.uid, self.password, self.instance_id, self.deployment_id, self.apikey, self.input_items)

        results = self.invoke_model(df, self.wml_endpoint, self.uid, self.password, self.instance_id, self.deployment_id, self.apikey, self.input_items)
        # results = np.ones(len(df))

        logging.debug('setting to 0' )
        df[self.output_items] = 0
        if results:
            logging.debug('results received' )
            # df.loc[:, self.output_items] = results['values']
            # df[self.output_items] = results['values']
            df[self.output_items] = [i[0] for i in results['values'] ]
        else:
            logging.error('error invoking external model')
        logging.debug(df[self.output_items].dtype.name)
        return df

        # logging.debug("exiting after model invoked")
        # return True
        # df = pd.DataFrame(data=table_data) # TODO, shouldn't have to query table, df generally holds the
        # num_rows = len(df)




    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UIMultiItem(
                    name = 'input_items',
                    datatype=float,
                    description = "Data items adjust",
                    # output_item = 'output_item',
                    is_output_datatype_derived = True)
                )
        # inputs.append(ui.UISingle(name='input_columns',
        #                       datatype=str,
        #                       description='Features to load from entity rows. Provide as list of comma seperated values like so - torque,speed,pressure',
        #                       tags=['TEXT'],
        #                       required=True
        #                       ))

        inputs.append(ui.UISingle(name='wml_endpoint',
                              datatype=str,
                              description='Endpoint to WML service where model is hosted',
                              tags=['TEXT'],
                              required=True
                              ))
        inputs.append(ui.UISingle(name='instance_id',
                              datatype=str,
                              description='Instance ID for WML model',
                              tags=['TEXT'],
                              required=True
                              ))
        inputs.append(ui.UISingle(name='deployment_id',
                              datatype=str,
                              description='Deployment ID for WML model',
                              tags=['TEXT'],
                              required=True
                              ))
        inputs.append(ui.UISingle(name='apikey',
                              datatype=str,
                              description='IBM Cloud API Key',
                              tags=['TEXT'],
                              required=True
                              ))
        # define arguments that behave as function outputs
        outputs=[]
        outputs.append(ui.UISingle(name='output_items', datatype=float))
        return (inputs, outputs)


        outputs = []
        return (inputs,outputs)
