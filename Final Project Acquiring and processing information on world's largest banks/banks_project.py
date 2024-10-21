# Code for ETL operations on Country-GDP data

#Import all libraries needed
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
import numpy as np
from datetime import datetime


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(f"{timestamp} : {message}\n")
    # print('Writing Log')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    # connect to the web
    response = requests.get(url)
    # Change html respon to text/html structure
    page = response.text
    # transform string to object
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attr_extr)
    table = data.find_all('tbody')
    rows = table[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            bank_name = col[1].find_all('a')
            bank_name = bank_name[-1].contents[0]
            total_asset = col[2].contents[0]
            # print(total_asset)
            # print(total_asset)
            data_dict = {"Name": bank_name,
                        "MC_USD_Billion": total_asset}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace(',','').str.strip()
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv('exchange_rate.csv')
    exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],0) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],0) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],0) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name,sql_connection, if_exists='replace')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    return query_output


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_file = 'code_log.txt'
log_progress("Declaring known values")
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attr_extr = ['Name','MC_USD_Billion']
table_attr_fnl = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_Banks'
log_progress('Preliminaries complete. Initiating ETL process')

log_progress('Call extract() function')
df = extract(url, table_attr_extr)
print(df)
log_progress('Data extraction complete. Initiating Transformation process')

log_progress('Call transform() function')
transform = transform(df, csv_path)
print(transform)
log_progress('Data transformation complete. Initiating Loading process')

log_progress('Call load_to_csv()')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

log_progress('Initiate SQLite3 connection')
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

log_progress('Call load_to_db()')
load_to_db(transform, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

log_progress('Call run_query()')
query_statement1 = f'SELECT * FROM {table_name}'
query_statement2 = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
query_statement3 = f'SELECT Name FROM {table_name} LIMIT 5'
print(run_query(query_statement1, sql_connection))
print(run_query(query_statement2, sql_connection))
print(run_query(query_statement3, sql_connection))
log_progress('Process Complete')

log_progress('Close SQLite3 connection')
sql_connection.close()
log_progress('Server Connection closed')


