import pandas as pd

from quantiphy import Quantity

from data_extraction import DataExtractor
from database_utils import DatabaseConnector

DBC = DatabaseConnector()
DE = DataExtractor()

#API endpoints
store_end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'


class DataCleaning:
    'This class implements data cleaning of data from different data sources.'
    def clean_user_data(self, df_user_data):
        """
        Function to clean users DataFrame and upload to the database
        
        Args:
        df_user_data (str) : pandas DataFrame name
        """
        #checking for NULL values
        print('============================================')
        df_user_data.info() #amount of null in each column
        print('============================================')
        #convert datetime
        df_user_data[['join_date','date_of_birth']] = df_user_data[['date_of_birth', 'join_date']].apply(pd.to_datetime, errors='coerce')
        

        clean_df_user_data = df_user_data.head()
        #upload clean data
        print('Uploading data ...')
        DBC.upload_to_db(clean_df_user_data, 'dim_users')

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
        products_df (str) : Name of the product DataFrame"""
        #Remove special characters from the price column
        products_df['product_price'] = products_df['product_price'].str.strip('£')

    def clean_orders_data(self):
        'Function to clean the orders data'
        orders_table = DE.extract_orders_data()
        #Drop columns
        orders_table = orders_table.drop(['first_name', 'last_name', '1'])

        #Upload the data
        clean_orders_table = orders_table.head()
        DBC.upload_to_db(clean_orders_table, 'orders_table')

    def called_clean_store_data(self):
        'Function to clean data from API and upload to database'
        
        store_data = pd.read_csv('store_data.csv')#DE.retrieve_stores_data(store_end_point)
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

    def clean_date_times(self):
        s3_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        dt_data = DE.extract_date_time_data(s3_link)

        #data cleaning

        #data upload
        clean_dt_data = dt_data.head()
        DBC.upload_to_db(clean_dt_data, 'dim_date_times')



            
            
                


        

if __name__ == '__main__':
    DC = DataCleaning()
    df = DataExtractor().retrieve_pdf_data('card_details.pdf')
    DC.clean_card_data(df)




# .dtypes -- returns the datatypes of the dataframe columns
# .info() -- returns the amount of null info in each column
# remove special characters using .str.strip() method
# .describe() -- gives the statistical info of a column
# .duplicated() -- checks if there are duplicates. with .sum() we can get the total
#   number of duplicated values
# .duplicated() takes on two arguments 
#       -- subset : list of columns to check for dup
#       -- keep : first, last, False