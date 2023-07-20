import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd
from prefect import flow, task
import os
import json
import gspread
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from gspread_dataframe import set_with_dataframe
import base64



@task(name='scraping', log_prints=True)
def extract_urls(ort):
    
    links = []
    dates = []

    globals()[f"df_{ort}"] = pd.read_csv(f"urls/urls_dk/urls_{ort}.csv", header = None)
    globals()[f"df_{ort}"]['https'] = 'https://www.donaukurier.de'
    globals()[f"df_{ort}"]['list_urls'] = globals()[f"df_{ort}"]['https'] + globals()[f"df_{ort}"][0]
    df = globals()[f"df_{ort}"] 

    for row in range(len(df)):
        URL = df['list_urls'][row]
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
        result = soup.find(class_="margin-right-10 d-block") 
        link = result.get('href')
        links.append(link)
    
    for row in range(len(df)):
        URL = links[row]
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
        result = soup.find(class_="date-published")
        date_in_string = result.text
        date2 = re.sub(r' ', '', date_in_string)
        date3 = re.sub(r'\n', '', date2)
        #date4 = re.sub(r'\.', '/', date3)
        first_chars = date3[0:10]
        dates.append(first_chars) 
    
    # create column with only gemeindenamen
    df['gemeinde'] = df[0].str.split('/',expand=True)[3]

    # add dates to df
    df['datum'] = dates

    # drop columns
    df = df.drop([0, 'https', 'list_urls'], axis=1)

    # convert to datetime 
    df['datum']=pd.to_datetime(df['datum'],format='%d.%m.%Y')

    df['Heute'] = pd.Timestamp("today").strftime("%Y/%m/%d")

    df['Heute'] = pd.to_datetime(df['Heute'])

    df['Letzter Artikel'] = df['Heute']-df['datum']

    df = df.drop(['Heute'], axis=1)

    df['Letzter Artikel'] = df['Letzter Artikel'].astype(str).str[:-5]#.astype('int')
    df['Letzter Artikel'] = df['Letzter Artikel'].astype('int')

    df = df.sort_values(by=['Letzter Artikel'], ascending=False).reset_index().drop(['index', 'datum'], axis=1)

    return df



@flow(name='create_dfs', log_prints=True)
def create_dfs():
    ei = extract_urls('ei')
    print('finished ei')
    keh = extract_urls('keh')
    print('finished keh')
    ne = extract_urls('ne')
    print('finished ne')
    nm = extract_urls('nm')
    print('finished nm')
    pf = extract_urls('pf')
    print('finished pf')
    ro = extract_urls('ro')
    print('finished ro')

    return ei, keh, ne, nm, pf, ro



@flow(name='clean df', log_prints=True)
def clean_data():
    df = create_dfs()
    df_final = pd.concat([df[0], df[1], df[2], df[3], df[4], df[5]], axis=1)
    df_final.fillna('-')
    df_final.columns = ['Eichstätt', 'Letzter Artikel', 'Kelheim', 'Letzter Artikel', 
                        'Neuburg', 'Letzter Artikel', 'Neumarkt', 'Letzter Artikel',
                        'Pfaffenhofen', 'Letzter Artikel', 'Roth', 'Letzter Artikel']
    df_final = df_final.fillna('-')
    return df_final



@flow(name='complete', log_prints=True)
def main_flow():
    df = clean_data()
    
    # Decode the base64-encoded string and load it as a JSON object
    credentials_json_str = base64.b64decode(os.environ['GOOGLE_APPLICATION_CREDENTIALS']).decode('utf-8')
    credentials_json = json.loads(credentials_json_str)
    credentials = service_account.Credentials.from_service_account_info(info=credentials_json, scopes=['https://www.googleapis.com/auth/spreadsheets'])

    # Initialize the Google Sheets client
    client = gspread.authorize(credentials)
    
    # Define the target Google Sheets file and worksheet
    sheet_id = '1ezE1LyZc8Y22ZMUUA2jyA3QWABm6OfgO6mC91gdThmQ'
    worksheet_name = 'Sheet1'

    # Open the Google Sheets file and worksheet
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(worksheet_name)

    # Clear the existing content in the worksheet
    worksheet.clear()

    # Save the DataFrame to the worksheet
    set_with_dataframe(worksheet, df)
    
    #file_path = 'gemeindeabdeckung_mz.xlsx'
    #with open(file_path, 'w') as file:
        #file.write(df.to_string(index=False))

    #df.to_excel('gemeindeabdeckung_mz.xlsx')
    print('done')



if __name__ == '__main__':
    main_flow()
