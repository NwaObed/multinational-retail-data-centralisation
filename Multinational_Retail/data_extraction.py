import pandas as pd
import requests

import tabula
import boto3

from .database_utils import DatabaseConnector

# APIs
header = {
    'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
}
num_store_end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'


class DataExtractor:
    '''This class is a data utility class that extracts
    data from multiple data sources'''
    def read_rds_table(self, DBC, table_name):
        """"
        Function to extract database table to pandas DataFrame
        
        Args:
        DBC (class) : Instance of Database Connector
        table_name (str) : Database table name
        """
        engine = DBC.init_db_engine('db_creds.yaml')
        return pd.read_sql_table(table_name, engine)

    def extract_user_data(self):
        'Function to extract data from database table and return pandas DataFrame.'
        DBC = DatabaseConnector()
        user_table = DBC.list_db_tables()
        return self.read_rds_table(DBC, user_table[1])


    def retrieve_pdf_data(self, link):
        """
        Extract data from pdf file and return pandas Dataframe
        
        Args:
        link (str) : pdf link"""
        return pd.DataFrame(tabula.read_pdf(link, pages='all')[0])
        

    def extract_from_s3(self, link):
        """
        Extract data from s3 bucket and return pandas DataFrame
        
        Args:
        link (str) : A link to AWS s3 bucket"""
        
        s3 = boto3.client('s3')
        cred = link.split('/')
        obj = s3.get_object(Bucket=cred[2].split('.')[0], Key=cred[-1])
        return pd.read_csv(obj.get('Body'))

    def extract_json_from_s3(self, link):
        'Extract json data from s3 bucket and return pandas DataFrame'
        
        s3 = boto3.client('s3')
        cred = link.split('/')
        obj = s3.get_object(Bucket=cred[2].split('.')[0], Key=cred[-1])
        return pd.read_json(obj.get('Body'))

    def extract_orders_data(self):
        'Extract orders data to pandas DataFrame'

        DBC = DatabaseConnector()
        return self.read_rds_table(DBC, 'orders_table')

    def list_number_of_stores(self, num_store_end_point, header):
        """
        This function gives the number of stores to extract data from
        
        Args:
        num_store_end_point (str): number of stores endpoint
        header (dict) : key-value pair of the header authorization
        
        Return:
        Number of stores to extract (int)"""
        
        response = requests.get(num_store_end_point, headers=header)
        return response.json()['number_stores']

    def retrieve_stores_data(self, store_end_point):
        """
        This function extracts all the store data from the API
        
        Arg:
        store_end_point (str) : A store endpoint to retrieve data from
        
        Return:
        DataFrame of the retrieved data."""
        number_stores = self.list_number_of_stores(num_store_end_point, header) #get the number of stores
        store_data = [] #list to store retrieved data  from each store
        #Iterate over the the store to retrieve data
        for store_number in range(number_stores):
            response = requests.get(store_end_point+str(store_number), headers=header)
            store_data.append(response.json()) #store response data
        df = pd.DataFrame(store_data)
        df.set_index('index', inplace=True)
        return df

    def extract_date_time_data(self,s3_date_link):
        """Function to extract date events from s3 bucket
        
        Args:
        s3_date_link (str) : s3 link to the data"""
        return self.extract_json_from_s3(s3_date_link)

    

