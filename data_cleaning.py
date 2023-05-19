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
        #checking for NULL values
        df_user_data.info() #amount of null in each column
        #data cleaning coming soon
        
        clean_df_user_data = df_user_data.head()
        #upload clean data
        DBC.upload_to_db(clean_df_user_data, 'dim_users')

    def clean_card_data(self,df_card_data):
        df_card_data.info()
        #data cleaning pending ...

        #upload clean data
        clean_df_card_data = df_card_data.head()
        DBC.upload_to_db(clean_df_card_data, 'dim_card_details')
    
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
        'Clean the products data'
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
        
        store_data = DE.retrieve_stores_data(store_end_point)

        #clean data
        
        #upload data
        clean_store_data = store_data.head()
        DBC.upload_to_db(clean_store_data, 'dim_store_details')

    def clean_date_times(self):
        s3_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        dt_data = DE.extract_date_time_data(s3_link)

        #data cleaning

        #data upload
        clean_dt_data = dt_data.head()
        DBC.upload_to_db(clean_dt_data, 'dim_date_times')



            
            
                


        

if __name__ == '__main__':
    DC = DataCleaning()
    #df = DataExtractor().read_rds_table()
    DC.clean_date_times(df)


# .dtypes -- returns the datatypes of the dataframe columns
# .info() -- returns the amount of null info in each column
# remove special characters using .str.strip() method
# .describe() -- gives the statistical info of a column
# .duplicated() -- checks if there are duplicates. with .sum() we can get the total
#   number of duplicated values
# .duplicated() takes on two arguments 
#       -- subset : list of columns to check for dup
#       -- keep : first, last, False