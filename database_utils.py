import yaml
from pathlib import Path
from sqlalchemy import create_engine, inspect
#from data_extraction import DataExtractor

# file = db_
class DatabaseConnector:
    'This class connects and uploads data to the database'
    def read_db_creds(self, file):
        with open(file, 'r') as my_file:
            creds = yaml.safe_load(my_file)
        #print(type(creds))
        return creds

    def init_db_engine(self):
        creds_dict = self.read_db_creds('db_creds.yaml') ##dict
        #print(creds_dict)
        engine = create_engine(f"postgresql+psycopg2://{creds_dict['RDS_USER']}:{creds_dict['RDS_PASSWORD']}@{creds_dict['RDS_HOST']}:{creds_dict['RDS_PORT']}/{creds_dict['RDS_DATABASE']}")
        return engine.connect()

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)

        return inspector.get_table_names()

    def upload_to_db(self,pd_dataframe,table_name):
        """
        This function uploads a DataFrame to Database table
        
        Args:
        pd_dataframe : DataFrame table to be uploaded
        table_name : Database table name to save the uploaded table"""
        engine = self.init_db_engine()
        print('Table uploading...')
        pd_dataframe.to_sql(table_name, engine, if_exists='replace')
        print('Table uploaded successfully')


if __name__ == '__main__':
    DBC = DatabaseConnector()
    #DBC.init_db_engine()
    print(DBC.list_db_tables())
    #DBC.upload_to_db()


#context managers?
#engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
#DATABASE_TYPE = 'postgresql'
#DBAPI = 'psycopg2'