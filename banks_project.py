import requests 
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np

DATA_URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
EXCHANGE_CSV_PATH = "exchange_rate.csv"
TABLE_ATRIBUTES_EXTRACTION = ["Name","MC_USD_BILLION"]
TABLE_ATRIBUTES_FINAL = ["Name","MC_USD_Billion","MC_GBP_Billion","MC_EUR_Billion","MC_INR_Billion"]
OUTPUT_CSV_PATH = "./Largest_banks_data.csv"
DATABASE_NAME = "Banks.db"
TABLE_NAME = "Largest_banks"
LOG_FILE = "    code_log.txt"

def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(LOG_FILE,"a") as f:
        f.write(timestamp+","+message+"\n")
        

def extract(url, table_attribs):
    try:
        webpage = requests.get(url)
        webpage.raise_for_status()  
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

    soup = BeautifulSoup(webpage.text, 'html.parser')
    df_data = []  

    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) > 2:
            bank_name = col[1].text.strip() 
            market_cap = float(col[2].text.strip())
            df_data.append({"Name": bank_name, "MC_USD_BILLION": market_cap})

    return pd.DataFrame(df_data, columns=table_attribs)

def transform(df, csv_path):
    exchange_rate = pd.read_csv(csv_path)   
    
    for rows in range(len(exchange_rate)):
        currency = exchange_rate.iloc[rows,0]
        df[f"MC_{currency}_BILLION"] = np.round(df["MC_USD_BILLION"] * float(exchange_rate.iloc[rows,1]),2)
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)
    
def load_to_db(df, sql_connection, table_name):
    conn = sqlite3.connect(sql_connection)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    
def run_query(query_statement, sql_connection):
    conn = sqlite3.connect(sql_connection)
    df = pd.read_sql_query(query_statement, conn)
    conn.close()
    print(query_statement)
    print("-")
    print(df)

try:
    df = extract(DATA_URL,TABLE_ATRIBUTES_EXTRACTION)
    log_progress("Data extraction complete. Initiating Transformation process")
except:
    log_progress("Data extraction incomplete")
    

try:
    df = transform(df,EXCHANGE_CSV_PATH)
    log_progress("Data transformation complete. Initiating Loading process")
except:
    log_progress("Data transformation incomplete")
    
    
try:
    load_to_csv(df, OUTPUT_CSV_PATH)
    log_progress("Data saved to CSV file")
except:
    log_progress("Data cannot be saved to CSV")
    

try:
    load_to_db(df, DATABASE_NAME, TABLE_NAME)
    log_progress("Data loaded to Database as a table, Executing queries")
except:
    log_progress("Data cannot be saved to database")
    
try:
    run_query("SELECT Name from Largest_banks LIMIT 5", DATABASE_NAME)
    log_progress("Process Complete")
except:
    log_progress("Process Uncomplete")