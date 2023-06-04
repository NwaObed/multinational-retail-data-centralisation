from Multinational_Retail.data_cleaning import DataCleaning
from Multinational_Retail.data_extraction import DataExtractor

DC = DataCleaning()
DE = DataExtractor()

# data links
pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
#API endpoints
store_end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
#S3 links
product_s3_link = 's3://data-handling-public/products.csv' #products link
dt_s3_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

#Extracting and cleaning orders data
print('*******************************EXTRACTING ORDERS DATA*******************************')
order_data = DE.extract_orders_data()
print('*******************************CLEANING ORDERS DATA*******************************')
DC.clean_orders_data(order_data)
print('*******************************CLEANING ORDERS DATA COMPLETE*******************************')

print('*******************************EXTRACTING USERS DATA*******************************')
users_data = DE.extract_user_data()
print('*******************************CLEANING USERS DATA*******************************')
DC.clean_user_data(users_data)
print('*******************************CLEANING USERS DATA COMPLETE*******************************')

print('*******************************EXTRACTING CARD DATA*******************************')
card_data = DE.retrieve_pdf_data(pdf_link)
print('*******************************CLEANING CARD DATA*******************************')
DC.clean_card_data(card_data)
print('*******************************CLEANING CARD DATA COMPLETE*******************************')

print('*******************************EXTRACTING PRODUCTS DATA*******************************')
product_data = DE.extract_from_s3(product_s3_link)
print('*******************************CLEANING PRODUCTS DATA*******************************')
product_data = DC.convert_product_weights(product_data) #weight conversion
DC.clean_products_data(product_data)
print('*******************************CLEANING PRODUCTS DATA COMPLETE*******************************')

print('*******************************EXTRACTING STORE DATA*******************************')
store_data = DE.retrieve_stores_data(store_end_point)
print('*******************************CLEANING STORE DATA*******************************')
DC.called_clean_store_data(store_data)
print('*******************************CLEANING STORE DATA COMPLETE*******************************')

print('*******************************EXTRACTING DATE-TIME DATA*******************************')
dt_data = DE.extract_date_time_data(dt_s3_link)
print('*******************************CLEANING DATE-TIME DATA*******************************')
DC.clean_date_times(dt_data)
print('*******************************CLEANING DATE-TIME DATA COMPLETE*******************************')