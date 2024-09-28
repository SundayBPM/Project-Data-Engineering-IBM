# Code for ETL operations on Country-GDP data

#Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. the
    function returns the dataframe for further processing.'''
    
    #Connect to the web
    response = requests.get(url)
    #Mengubah respon menjadi text/struktur web
    page = response.text
    #Mengubah string ke objek
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    table = data.find_all('tbody')
    rows = table[2].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0 :
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Million) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    df = df
    df["GDP_USD_millions"] = df["GDP_USD_millions"].str.replace(',','').astype(int)
    df["GDP_USD_millions"] = np.round(df["GDP_USD_millions"]/1000,2)
    df = df.rename(columns={"GDP_USD_millions":"GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a 'CSV' file
    in the provided path. Funtion returns nothing.'''
    df.to_csv(csv_path)
    return None


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing. '''
    df.to_sql(table_name, con=sql_connection, if_exists='replace', index=False)
    return None

def run_query(query_statement, sql_connection):
    ''' this function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing.'''
    print(query_statement)
    df = pd.read_sql(query_statement, con=sql_connection)
    print(df)
    return None


def log_progress(message):
    ''' This function logs the mentioned message at 
    a given stage of the code execution to a log file. 
    Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.log","a") as f:
        f.write(timestamp + ' : ' + message + '\n')
    return None

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not insedie any function.'''

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'
log_progress('Preliminaries complete. Initiating ETL process.')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process.')

df=transform(df)
log_progress('Data transformation complete. Initiating loading process.')

load_to_csv(df, csv_path)
log_progress('Data saved to CSV file.')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query.')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()
