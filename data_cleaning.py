import pandas as pd
import numpy as np
import warnings

from quantiphy import Quantity

from data_extraction import DataExtractor
from database_utils import DatabaseConnector

warnings.filterwarnings('ignore')
DBC = DatabaseConnector()
DE = DataExtractor()

#API endpoints
store_end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

#S3 links
product_s3_link = 's3://data-handling-public/products.csv' #products link
dt_s3_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

class DataCleaning:
    'This class implements data cleaning of data from different data sources.'
    def clean_user_data(self, df_user_data):
        """
        Function to clean users DataFrame and upload to the database
        
        Args:
        df_user_data (str) : pandas DataFrame name
        """
        #checking for NULL values
        print('=================================General Info===============================')
        print(df_user_data.info()) #amount of null in each column
        print('=================================Total missing values===============================')
        #check total missing values
        print(df_user_data.isna().sum())
        #investigate country_code
        print('=================================Unique Country codes===============================')
        print(df_user_data.country_code.unique().tolist())
        df_user_data.country_code = df_user_data.country_code.replace(to_replace=['GGB'], value=['GB'])
        invalid_users_data = df_user_data[~df_user_data.country_code.isin(['DE','GB','US'])]
        print('============================== Invalid data =========================================')
        print(invalid_users_data)
        #Drop invalid data
        df_user_data = df_user_data.drop(index=invalid_users_data.index)
        #convert datetime
        df_user_data[['join_date','date_of_birth']] = df_user_data[['date_of_birth', 'join_date']].apply(pd.to_datetime, errors='coerce')
        print('=================================General info after cleaning===============================')
        print(df_user_data.info()) #amount of null in each column
        #upload clean data
        print('Uploading data ...')
        DBC.upload_to_db(df_user_data, 'dim_users')

    def clean_card_data(self,df_card_data):
        """
        Function to clean card details.
        
        Arg:
        df_card_data (str) : Name of the card DataFrame data"""
        print('================Check for null values=====================')
        print(df_card_data.info())
        #Investigate individual columns for data consistency
        print('================Checking Card Numbers=====================')
        print(df_card_data.card_number.unique())
        print('================Checking expiry_date=====================')
        print(df_card_data.expiry_date.unique())
        print('================Checking card_provider=====================')
        print(df_card_data.card_provider.unique())
        print('================Checking date_payment_confirmed=====================')
        print(df_card_data.date_payment_confirmed.unique())

        #upload clean data
        DBC.upload_to_db(df_card_data, 'dim_card_details')
    
    def convert_product_weights(self, products_df):
        'Convert all product weights to kg and return dataframe'
        
        def __strip_unit(value, unit):
            'Function to remove units and convert string values to float'
            return float(value.strip(unit))
        
        #loop through the weight column to update values
        for idx,val in enumerate(products_df['weight']):
            if type(val) is float: #value in required format, no action taken
                continue
            elif len(val.split()) == 1:
                if val[-2:] == 'kg':
                    #convert values in kg to numerical value
                    products_df['weight'].loc[idx] = __strip_unit(val,val[-2:])
                elif val[-1] == 'g':
                    #convert values in g to numerical value
                    products_df['weight'].loc[idx] = __strip_unit(val,val[-1])/1000.0
                elif val[-2:] == 'ml':
                    #use 1:1 ratio to convert values in ml to numericals
                    products_df['weight'].loc[idx] = __strip_unit(val,val[-2:])/1000.0
                elif val[-2] == 'oz':
                    #use 1:1 ratio to convert values in oz to numerical values
                    products_df['weight'].loc[idx] = __strip_unit(val,val[-2:])/1000.0
            elif len(val.split()) == 2:
                val = val.split()
                products_df['weight'].loc[idx] = __strip_unit(val[0],'g')/1000.0
            else:
                len(val.split()) > 2
                val = val.split()
                products_df['weight'].loc[idx] = (__strip_unit(val[0], 'g')*__strip_unit(val[-1], 'g'))/1000.0

        return products_df

    def clean_products_data(self, products_df):
        """Function to clean the products data
        
        Arg:
        products_df (str) : Name of the product DataFrame returned from convert_product_weights()"""
        #Remove special characters from the price column
        products_df['product_price'] = products_df['product_price'].str.strip('£')
        invalid_price_data = products_df[products_df.product_price.isin([np.nan, 'XCD69KUI0K', 'N9D2BZQX63', 'ODPMASE7V7', ])] #get rows of invalid country codes
        print('=====================Invalid rows=============================')
        print(invalid_price_data)
        print('==================================================================')
        # Drop the invalid data rows
        products_df = products_df.drop(index=invalid_price_data.index)
        #convert to float
        products_df.product_price = products_df.product_price.astype(float)
        #Investigate weight
        products_df.weight[products_df['weight'] == '16oz'] = '0.016'
        products_df.weight = products_df.weight.astype(float)
        # #Investigate date_added
        products_df.date_added.unique().tolist()
        products_df.date_added = pd.to_datetime(products_df.date_added, format='mixed')
        #Upload the data
        DBC.upload_to_db(products_df, 'dim_products')
        # return products_df


    def clean_orders_data(self, orders_table):
        'Function to clean the orders data'
        orders_table = orders_table.set_index('index')#set index
        #Drop columns
        orders_table = orders_table.drop(['first_name', 'last_name', '1'],axis=1)

        #Upload the data
        DBC.upload_to_db(orders_table, 'orders_table')

    def called_clean_store_data(self,store_data):
        'Function to clean data from API and upload to database'
        
        store_data2 = store_data #make a copy
        # data cleaning
        print('========================Number of missing values ==============================')
        #Number of missing values
        print(store_data2.isna().sum())
        #investigate country code
        print('======================== Country code unique values ==============================')
        print(store_data2.country_code.unique())
        invalid_country_codes_row = store_data2[~store_data2.country_code.isin(['GB','DE','US'])] #get rows of invalid country codes
        print(invalid_country_codes_row)
        #DROP ROWS WITH INVALID COUNTRY CODES
        store_data2 = store_data2.drop(index=invalid_country_codes_row.index)
        #investigate continent
        print('======================== Continent unique values ==============================')
        print(store_data2.continent.unique())
        store_data2.continent = store_data2.continent.replace(to_replace=['eeEurope', 'eeAmerica'], value=['Europe', 'America']) 
        #investigate lat
        print('======================== Lat unique values ==============================')
        print(store_data2.lat.unique())
        #Drop lat -- contains only None and NA values for 440 of 441 rows
        store_data2 = store_data2.drop('lat', axis=1)
        #Investigate staff_numbers
        print('======================== Staff Numbers unique values ==============================')
        print(store_data2.staff_numbers.unique())
        store_data2=store_data2.replace(to_replace=['N/A',None], value=0.0) #replace N/A
        store_data2['staff_numbers']=store_data2['staff_numbers'].str.extract('(\d+)').astype(int) #convert alphanumeric to numeric
        store_data2[['longitude','latitude']] = store_data2[['longitude','latitude']].astype('float64') #Convert str to float
        print('======================== Check number of NA in clean data ==============================')
        print(store_data2.isna().sum())

        #upload clean data to database
        DBC.upload_to_db(store_data2, 'dim_store_details')

    def clean_date_times(self, dt_df):
        """
        Function to clean date_time data and upload the DataFrame to database
        
        Arg:
        dt_df (str) : Name of the date_time DataFrame"""

        #data cleaning
        print('===============================Data Headers===============================')
        print(dt_df.columns)
        #missing values
        print('===============================Total Missing Values===============================')
        dt_df.isna().sum()
        #Investigate month column
        print('===============================Month Unique values===============================')
        print(dt_df.month.unique())
        invalid_dt_df = dt_df[~dt_df.month.isin(['1','2','3','4','5','6','7','8','9','10','11','12'])]
        print('===============================Messy rows===============================')
        print(invalid_dt_df)
        #Drop the invalid data
        dt_df = dt_df.drop(index=invalid_dt_df.index)
        #data upload
        DBC.upload_to_db(dt_df, 'dim_date_times')

if __name__ == '__main__':
    DC = DataCleaning()
    DE = DataExtractor()
    print('*******************************CLEANING USERS DATA*******************************')
    users_data = DE.extract_user_data()
    DC.clean_user_data(users_data)
    print('*******************************CLEANING USERS DATA COMPLETE*******************************')


    print('*******************************CLEANING CARD DATA*******************************')
    card_data = DE.retrieve_pdf_data('card_details.pdf')
    DC.clean_card_data(card_data)
    print('*******************************CLEANING CARD DATA COMPLETE*******************************')
    
    print('*******************************CLEANING PRODUCTS DATA*******************************')
    product_data = DE.extract_from_s3(product_s3_link)
    product_data = DC.convert_product_weights(product_data) #weight conversion
    DC.clean_products_data(product_data)
    print('*******************************CLEANING PRODUCTS DATA COMPLETE*******************************')

    print('*******************************CLEANING ORDERS DATA*******************************')
    order_data = DE.extract_orders_data()
    DC.clean_orders_data(order_data)
    print('*******************************CLEANING ORDERS DATA COMPLETE*******************************')

    print('*******************************CLEANING STORE DATA*******************************')
    store_data = DE.retrieve_stores_data(store_end_point)
    DC.called_clean_store_data(store_data)
    print('*******************************CLEANING STORE DATA COMPLETE*******************************')

    print('*******************************CLEANING DATE-TIME DATA*******************************')
    dt_data = DE.extract_date_time_data(dt_s3_link)
    DC.clean_date_times(dt_data)
    print('*******************************CLEANING DATE-TIME DATA COMPLETE*******************************')



# .dtypes -- returns the datatypes of the dataframe columns
# .info() -- returns the amount of null info in each column
# remove special characters using .str.strip() method
# .describe() -- gives the statistical info of a column
# .duplicated() -- checks if there are duplicates. with .sum() we can get the total
#   number of duplicated values
# .duplicated() takes on two arguments 
#       -- subset : list of columns to check for dup
#       -- keep : first, last, False