import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename="logs/ingestion.db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine ("mysql+pymysql://root:Vipul%40123@localhost:3306/sales")
folder_path=(r"E:\Project\data")

def ingest_db(df, table_name,engine):
    '''This function will ingest the dataframe into database table'''
    df.to_sql(table_name, con = engine, if_exists='replace',index=False)

def load_raw_data():
    '''This function will load the CSVs as dataframe and ingest into db'''
    start = time.time()
    for file in os.listdir(folder_path):
        if '.csv' in file:
             for chunk in pd.read_csv(
            os.path.join(folder_path, file),
            chunksize=100000):
              ingest_db(chunk, file[:-4], engine)
    end = time.time()        
    total_time = (end - start)/60 
    logging.info('Ingestion Complete')  

    logging.info(f'\nTotal Time Taken : {total_time} minutes')       

if __name__ == '__main__':
     load_raw_data()
     