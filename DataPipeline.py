import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
import logging
from lib.Utils import Utils
from pandas import json_normalize, to_datetime
from shutil import rmtree

class DataPipeline:

    def __init__(self):

        self.BASE_DIR = os.path.dirname(os.path.dirname( os.path.abspath(__file__) ) )
        self.SRC_DIR = os.path.join(self.BASE_DIR, 'src')
        self.DATA_DIR = os.path.join(self.SRC_DIR, 'data')
        self.TMP_DIR =  os.path.join(self.DATA_DIR, 'tmp')
        self.DATA_LOG_DIR =  os.path.join(self.TMP_DIR, 'data_log_req')

        self.utils = Utils()

    def execute(self):

        self.utils.extractZip(os.path.join(self.DATA_DIR, 'data_log_req.zip'), self.TMP_DIR)

        UNZIP_PATH = os.path.join(self.DATA_LOG_DIR, "logs.txt")

        with open(UNZIP_PATH) as file:
            data = [json.loads(line) for line in file]
        
        dataframe = pd.json_normalize(data)

        dataframe.columns = self.utils.columns_names(dataframe.columns)

        columns = [
        'service_updated_at',
        'service_created_at',
        'route_updated_at',
        'route_created_at',
        'started_at']

        dataframe[columns] = dataframe[columns].apply(to_datetime, unit='s')

        json_columns = [
        'route_methods',
        'route_paths',
        'route_protocols',
        'request_querystring']
        for c in json_columns:
            dataframe[c] = dataframe[c].apply(json.dumps)

        try:
            self.utils.loadToDataBase(schema='etl_eng', table='data_log', df=dataframe)
        except sqlalchemy.exc.OperationalError as e:
            logger.info('Error - {}'.format(e.args))

        rmtree(self.TMP_DIR)
        print("Finished")
        return


if __name__ == '__main__':

    data_pipe = DataPipeline()
    data_pipe.execute()