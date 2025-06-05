from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging



from networksecurity.entity.config_entiy import DataIngestionConfig

from networksecurity.entity.artifact_entity import DataIngestionArtifact
import os
import sys
import numpy as np
import pandas as pd
import pymongo

from typing import List

from sklearn.model_selection import train_test_split

from dotenv import load_dotenv 

import certifi

load_dotenv()

MONGODB_DB_URL = os.getenv("MONGODB_DB_URL")

ca = certifi.where()

class DataIngestion:
    def __init__(self,data_ingestion_config:DataIngestionConfig):
        self.data_ingestion_config = data_ingestion_config

    def export_collection_as_dataframe(self):
        """
        Read data from mongodb
        """
        try:
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name

            self.mongo_client = pymongo.MongoClient(MONGODB_DB_URL,tlsCAFile=ca)
            collection = self.mongo_client[database_name][collection_name]

            df = pd.DataFrame(list(collection.find()))

            if "_id" in df.columns.tolist():
                df = df.drop(columns=["_id"],axis=1)

            df.replace({"na":np.nan},inplace=True)

            return df
        except Exception as e:
            raise NetworkSecurityException(e,sys)

   
    def export_data_into_feature_store(self,dataframe: pd.DataFrame):
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            dataframe.to_csv(feature_store_file_path,index=False,header=True)
            return dataframe
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def split_data_as_train_test(self,dataframe: pd.DataFrame):
        try:
            train_set,test_set = train_test_split(
                dataframe, test_size= self.data_ingestion_config.train_test_split_ratio
            )

            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path,exist_ok=True)

            train_set.to_csv(
                self.data_ingestion_config.training_file_path, index= False, header= True
            )

            train_set.to_csv(
                self.data_ingestion_config.testing_file_path, index= False, header= True
            )

        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
   
        
    def initiate_data_ingestion(self):
        try:
            dataframe=self.export_collection_as_dataframe()
            dataframe=self.export_data_into_feature_store(dataframe)
            self.split_data_as_train_test(dataframe)
            dataingestionartifact=DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path,
                                                        test_file_path=self.data_ingestion_config.testing_file_path)
            return dataingestionartifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)