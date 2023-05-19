import pandas as pd
import requests

import tabula
import boto3

from database_utils import DatabaseConnector

# data links
pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
s3_link = 's3://data-handling-public/products.csv'

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
        # DBS = DatabaseConnector()
        # table = DBS.list_db_tables()
        engine = DBC.init_db_engine()
        # print(table)
        # legacy_users = pd.read_sql_table(table[1], engine)

        return pd.read_sql_table(table_name, engine)


    def retrieve_pdf_data(self, link=pdf_link):
        'Extract data from pdf and return pandas Dataframe'

        df = tabula.read_pdf(link, pages='all')
        return df

    def extract_from_s3(self, link=s3_link):
        'Extract data from s3 bucket and return pandas DataFrame'
        
        s3 = boto3.client('s3')
        cred = s3_link.split('/')
        obj = s3.get_object(Bucket=cred[2].split('.')[0], Key=cred[-1])
        df = pd.read_csv(obj.get('Body'))

        return df

        self.read_rds_table()
        print('===========================================')
        print(f'List of tables : {self.read_rds_table().table}')

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
        number_stores = self.list_number_of_stores(num_store_end_point, header) #get the number of stores
        store_data = []
        #Iterate over the the store to retrieve data
        for store_number in range(number_stores):
            response = requests.get(store_end_point+str(store_number), headers=header)
            store_data.append(response.json())
        df = pd.DataFrame(store_data)
        df.set_index('index', inplace=True)
        return df

    def extract_date_time_data(self,s3_date_link):
        """Function to extract date events from s3 bucket
        
        Args:
        s3_date_link (str) : s3 link to the data"""
        return self.extract_from_s3(s3_date_link)



# table = DBS.list_db_tables()
# print(table)


if __name__ == '__main__':
    DE = DataExtractor()
    # orders_table = DE.extract_orders_data()
    # print(orders_table.head())
    # print(orders_table.columns)
    #DBC = DatabaseConnector()
    # table = DE.read_rds_table()
    #print(DE.list_db_tables())
    # DE.read_rds_table()
    # print(f'List of tables : {DE.read_rds_table().table}')
    #print(table.head())
    #print(DE.read_rds_table().table)
    # number_stores = 4# self.list_number_of_stores(num_store_end_point, header)
    # store_end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    # df = DE.retrieve_stores_data(store_end_point)
    s3_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    df = DE.extract_date_time_data(s3_link)
    print(df)
