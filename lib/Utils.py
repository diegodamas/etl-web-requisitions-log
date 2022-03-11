import os
import pandas as pd
import sqlalchemy
import dotenv
import json
from zipfile import ZipFile
import re

class Utils:

    def __init__(self):

        dotenv.load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)),'.env'))
        self.__user = os.getenv('USER')
        self.__psw = os.getenv('PSW')
        self.__host = os.getenv('HOST')
        self.__port = os.getenv('PORT')

        self.engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.__user}:{self.__psw}@{self.__host}:{self.__port}")
        self.connection = self.engine.connect()

    def loadToDataBase(self, schema, table, df):
        df.to_sql(schema=schema, name=table, con = self.engine, if_exists="replace", index=False)

    def extractZip(self, input_path, extract_path):
        with ZipFile(input_path, 'r') as zip_file:
            zip_file.extractall(extract_path)

    def query_import(self, path, **kwargs):
        with open(path, 'r', **kwargs) as file_query:
            query = file_query.read()
            df = read_sql(query, self.connection)
            return df
    
    def list_to_json(self, dataframe, json_labels):
        for labels in json_labels:
            dataframe[labels] = dataframe[labels].apply(json.dumps)
        return dataframe

    def columns_names(self, df_columns):
        return [re.sub("[.-]", "_", c).lower() for c in df_columns]
