import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
import numpy as np
import pandas as pd

from iotfunctions.base import BaseTransformer
from iotfunctions import ui

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install

PACKAGE_URL = 'git+https://github.com/IBM/iot-analytics-anomaly@'

class IsolationForestModelWMLTest(BaseTransformer):

    def __init__(self, input_items, output_item = 'http_preload_done'):

        self.input_items = input_items
        self.output_item = output_item
        super().__init__()
    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[input_items[0]]
        return df

    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UIMultiItem(
                    name = 'input_items',
                    datatype=float,
                    description = "Data items adjust",
                    output_item = 'output_item',
                    is_output_datatype_derived = True)
                )

        outputs = []
        return (inputs,outputs)
