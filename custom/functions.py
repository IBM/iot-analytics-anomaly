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
#PACKAGE_URL = 'git+https://github.com/madendorff/functions@'
PACKAGE_URL = 'git+https://github.com/kkbankol-ibm/monitor-anomaly@'

class InvokeExternalModel(BasePreload):
    '''
    Do a HTTP request as a preload activity. Load results of the get into the Entity Type time series table.
    HTTP request is experimental
    '''

    out_table_name = None

    # def __init__(self, username, password, request, model_url, headers = None, body = None, column_map = None, output_item  = 'http_preload_done'):
    def __init__(self, model_url, headers = None, body = None, column_map = None, output_item  = 'http_preload_done'):
        if body is None:
            body = {}

        if headers is None:
            headers = {}

        if column_map is None:
            column_map = {}

        super().__init__(dummy_items=[],output_item = output_item)

        # create an instance variable with the IBM IOT Platform Analytics Service Function input arguments.

        # self.username = username
        # logging.debug('self.username %s' %self.username)
        # self.url = url
        # self.password = password
        # logging.debug('self.password %s' %self.password)
        # # self.tenant = url
        # # logging.debug('tenantid self.tenant %s' %self.tenant)
        # self.request = request
        # logging.debug('self.request %s' %self.request)
        # self.headers = headers
        # logging.debug('headers %s' %headers)
        logging.debug('model_url %s' %model_url)
        self.model_url = model_url
        self.body = body
        logging.debug('body %s' %body)
        self.column_map = column_map
        logging.debug('column_map %s' %column_map)

        # TODO, add this section after adding basic auth to section
        # if headers:
        #     self.headers['auth'] = self.token
        # else:
        #     self.headers = {
        #         "auth": self.token
        #     }


        # return metrics_value, metrics_unit, metrics_compare_percent, metrics_trend, metrics_trend_status


        # '''
        # [
        # "buildingName":
        #     {
        #         "temp": "75",
        #         "energy": "15kw"
        #     }
        # ]
        # '''
        #
        # '''
        # # input list of buildings and Returns list of energy by building.
        #
        # # wastage: Provides energy wastage of this building for last 30 days. Provides % Wastage compared to total energy usage of that building. Wastage is calculated as the sum of excess energy consumed over the upper bound of the predicted energy, in the last 30 days.
        # {
        #   "value": 0,
        #   "unit": "string",
        #   "usage_percent": 0
        # }
        #
        # # usage: Provides energy consumption of this building for last 30 days. Provide % Up or Down compared to same 30 days of the last year
        # {
        #   "value": 0,
        #   "unit": "string",
        #   "compare_percent": 0,
        #   "trend": "string",
        #   "trend_status": "string"
        # }
        #
        # # prediction:  Returns the energy usage for last 48 hours, energy prediction for next 48 hours and the trend whether its up or down
        # {
        #   "value": 0,
        #   "unit": "string",
        #   "trend": "string",
        #   "trend_status": "string",
        #   "last_value": 0,
        #   "last_unit": "string"
        # }
        # '''
        #
        # '''
        # # Initialize building energy metrics to retrieve
        # '''
        # metrics_value = []
        # metrics_unit  = []
        # metrics_compare_percent  = []
        # metrics_trend = []
        # metrics_trend_status = []
        #
        # logging.debug("Getting Energy")
        # header = {}
        # # auth_str = 'Bearer '+ self.token
        # #header = { 'Authorization':  }
        # header['maxauth'] = self.token
        # body = {}
        # energy_data_options = ['usage']
        #
        #
        # for bldg in buildings:
        #     logging.debug("getMeters for buiding %s "  %bldg)
        #
        #     for etype in energy_data_options:
        #         logging.debug("getMeters type %s " %etype  )
        #         uri = "https://" + self.tenant + "-agg.mybluemix.net/api/v1/building/energy/" + etype
        #         logging.debug("uri %s" %uri)
        #         req = self.db.http.request('GET',
        #                          uri,
        #                          fields={'buildingName': bldg
        #                                  },
        #                          body=body,
        #                          headers= header)
        #         if req.status == 200:
        #             logging.debug("energy_metrics req.data  %s" %req.data )
        #             # '{"value":16.3,"unit":"MWh","compare_percent":7.34,"trend":"DOWN","trend_status":"GREEN"}'
        #             energy_metrics_json = json.loads(req.data.decode('utf-8'))
        #             metrics_value.append(energy_metrics_json['value'])
        #             metrics_unit.append(energy_metrics_json['unit'])
        #             metrics_compare_percent.append(energy_metrics_json['compare_percent'])
        #             metrics_trend.append(energy_metrics_json['trend'])
        #             metrics_trend_status.append(energy_metrics_json['trend_status'])
        #         else:
        #             logging.debug('energy_metrics no data found' )
        #             metrics_value.append(0.0)
        #             metrics_unit.append("NA")
        #             metrics_compare_percent.append(0.0)
        #             metrics_trend.append("NA")
        #             metrics_trend_status.append("NA")
        #
        # return metrics_value, metrics_unit, metrics_compare_percent, metrics_trend, metrics_trend_status

    # def parseBuildings (self, data = None ):
    #     buildings = []
    #     for bldg in data:
    #         logging.debug("parseBuildings  bld %s " %bldg)
    #         if '_' not in bldg['src'] :
    #             #building['id'] = bldg['src']
    #             buildings.append(bldg['src'])
    #     return buildings


    def invoke_model(self, df):
        logging.debug('invoking model')
        model_url = self.model_url
        body = df.to_dict()
        logging.debug('posting dataframe %s' %str(body))
        logging.debug('target %s' %model_url)
        # print("posting following dataframe")
        # print(body)
        # here we need to filter down to the specific fields the user wants.
        r = requests.post(model_url, json=body)
        if r.status_code == 200:
            logging.debug("predictions received")
            predictions = r.json()
            logging.debug("predictions")
            logging.debug(predictions)
            return predictions
        else:
            logging.debug("failure receiving predictions")
            logging.debug(r.status_code)
            logging.debug(r.text)
            return []

    def execute(self, df, start_ts = None,end_ts=None,entities=None):
        logging.debug('in execution method')
        entity_type = self.get_entity_type()
        logging.debug('entity_type')
        self.db = entity_type.db
        logging.debug('entity db')
        # encoded_body = json.dumps(self.body).encode('utf-8')
        # encoded_headers = json.dumps(self.headers).encode('utf-8')

        # This class is setup to write to the entity time series table
        # To route data to a different table in a custom function,
        # you can assign the table name to the out_table_name class variable
        # or create a new instance variable with the same name

        if self.out_table_name is None:
            table = entity_type.name
        else:
            table = self.out_table_name
        logging.debug('set table')
        schema = entity_type._db_schema
        logging.debug('schema')

        # logging.debug('looking for anamoly in %s ' %m)
        df = pd.DataFrame({"speed": [30, 40, 50], "work_completed": [10,2,7]})
        logging.debug("printing df")
        logging.debug(df)
        logging.debug(f'df shape {df.shape}')

        predictions = self.invoke_model(df)
        logging.debug('predictions %s' %predictions )

        # rows = len(buildings)
        # logging.debug('rows %s ' %rows)
        response_data = {}
        (metrics,dates,categoricals,others) = self.db.get_column_lists_by_type(
            table = table,
            schema= schema,
            exclude_cols = []
        )
        # TODO, overriding metrics for now
        metrics = ['speed']
        for m in metrics:
            logging.debug('metrics %s ' %m)
            response_data[m] = np.random.normal(0,1,rows)
            logging.debug('metrics data %s ' %response_data[m])

        for d in dates:
            logging.debug('dates %s ' %d)
            response_data[d] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
            logging.debug('dates data %s ' %response_data[d])

        '''
        # Create Numpy array
        '''
        response_data['speed'] = np.array([30, 40, 50])
        # response_data['speed_KB_Robot_Type_max'] = np.array([])
        # response_data['predictions'] = np.array(predictions) # TODO, need to add scores as well, likelihood of anamoly
        ## TODO, not sure what the following values should be?
        '''
        # Create a timeseries dataframe with data received from Maximo
        '''
        logging.debug('response_data used to create dataframe ===' )
        logging.debug( response_data)
        df = pd.DataFrame(data=response_data)
        logging.debug('Generated DF from response_data ===' )
        logging.debug( df.head() )
        df = df.rename(self.column_map, axis='columns')
        logging.debug('ReMapped DF ===' )
        logging.debug( df.head() )

        '''
        # Fill in missing columns with nulls
        '''
        required_cols = self.db.get_column_names(table = table, schema=schema)
        logging.debug('required_cols %s' %required_cols )
        missing_cols = list(set(required_cols) - set(df.columns))
        logging.debug('missing_cols %s' %missing_cols )
        if len(missing_cols) > 0:
            kwargs = {
                'missing_cols' : missing_cols
            }
            entity_type.trace_append(created_by = self,
                                     msg = 'http data was missing columns. Adding values.',
                                     log_method=logger.debug,
                                     **kwargs)
            for m in missing_cols:
                if m==entity_type._timestamp:
                    df[m] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
                elif m=='devicetype':
                    df[m] = entity_type.logical_name
                else:
                    df[m] = None

        '''
        # Remove columns that are not required
        '''
        df = df[required_cols]
        logging.debug('DF stripped to only required columns ===' )
        logging.debug( df.head() )

        '''
        # Write the dataframe to the IBM IOT Platform database table
        '''
        self.write_frame(df=df, table_name=table)
        kwargs ={
            'table_name' : table,
            'schema' : schema,
            'row_count' : len(df.index)
        }
        entity_type.trace_append(created_by=self,
                                 msg='Wrote data to table',
                                 log_method=logger.debug,
                                 **kwargs)
        return True

    '''
    # Create the IOT Platform Function User Interfact input arguements used to connect to the external REST Service.
    # These could be used to connect with any Rest Service to get IOT Data or any other data to include in your dashboards.
    '''
    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        # define arguments that behave as function inputs
        inputs = []
        # inputs.append(ui.UISingle(name='username',
        #                       datatype=str,
        #                       description='Username for Maximo Instance',
        #                       tags=['TEXT'],
        #                       required=True
        #                       ))
        # inputs.append(ui.UISingle(name='password',
        #                       datatype=str,
        #                       description='Password for Maximo Instance',
        #                       tags=['TEXT'],
        #                       required=True
        #                       ))
        inputs.append(ui.UISingle(    name='model_url',
                                      datatype=str,
                                      description='url of external hosted model',
                                      tags=['TEXT'],
                                      required=True
                                  ))
        # define arguments that behave as function outputs
        outputs=[]
        outputs.append(ui.UIStatusFlag(name='output_item'))
        return (inputs, outputs)
