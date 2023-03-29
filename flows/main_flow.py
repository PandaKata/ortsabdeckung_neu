import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from prefect import flow, task
import os
import json
import gspread
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from gspread_dataframe import set_with_dataframe

#https://medium.com/the-prefect-blog/scheduled-data-pipelines-in-5-minutes-with-prefect-and-github-actions-39a5e4ab03f4

@task(name='scraping', log_prints=True)
def extract_urls(ort):

    links = []
    dates = []

    globals()[f"df_{ort}"] = pd.read_csv(f"urls_{ort}.csv", header = None)
    globals()[f"df_{ort}"]['https'] = 'https://www.mittelbayerische.de'
    globals()[f"df_{ort}"]['list_urls'] = globals()[f"df_{ort}"]['https'] + globals()[f"df_{ort}"][0]
    df = globals()[f"df_{ort}"] 

    for row in range(len(df)):
        URL = df['list_urls'][row]
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
        result = soup.find(class_="teaser teaser-33-lead") 
        link = result.find('a')['href']
        links.append(link)
    
    # create URLs, add https... 
    append_str = 'https://www.mittelbayerische.de'

    new_articles = [append_str + sub for sub in links]

    for row in range(len(new_articles)):
        URL = new_articles[row]
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
        result = soup.find(class_="date")
        date = result.text
        dates.append(date) 
    
    # create column with only gemeindenamen
    df['gemeinde'] = df[0].str.split('/',expand=True)[4]

    # add dates to df
    df['datum'] = dates

    # drop columns
    df = df.drop([0, 'https', 'list_urls'], axis=1)

    # convert to datetime 
    # format must be international
    d = {'Januar': 'Jan', 'Februar': 'Feb', 'März': 'Mar', 'April': 'Apr', 'Mai':'May', 'Juni': 'Jun', 'Juli': 'Jul', 'August': 'Aug', 'September': 'Sep', 'Oktober':'Oct', 'November': 'Nov', 'Dezember':'Dec'}
    df['datum']=pd.to_datetime(df['datum'].replace(d, regex=True), errors='coerce')

    df['Heute'] = pd.Timestamp("today").strftime("%Y/%m/%d")
    df['Heute'] = pd.to_datetime(df['Heute'])
    df['Letzter Artikel'] = df['Heute']-df['datum']

    df = df.drop(['Heute'], axis=1)

    df['Letzter Artikel'] = df['Letzter Artikel'].astype(str).str[:-5].astype('int')

    df = df.sort_values(by=['Letzter Artikel'], ascending=False).reset_index().drop(['index', 'datum'], axis=1)

    return df



@flow(name='create_dfs', log_prints=True)
def create_dfs():
    rgb = extract_urls('rgb')
    print('finished rgb')
    cha = extract_urls('cha')
    print('finished cham')
    nm = extract_urls('nm')
    print('finished nm')
    keh = extract_urls('keh')
    print('finished keh')
    sad = extract_urls('sad')
    print('finished sad')
    am = extract_urls('am')
    print('am finished')

    return rgb, cha, nm, keh, sad, am

@flow(name='clean df', log_prints=True)
def clean_data():
    df = create_dfs()
    df_final = pd.concat([df[0], df[1], df[2], df[3], df[4], df[5]], axis=1)
    df_final.fillna('-')
    df_final.columns = ['Regensburg', 'Letzter Artikel', 'Cham', 'Letzter Artikel', 
                        'Neumarkt', 'Letzter Artikel', 'Kelheim', 'Letzter Artikel',
                        'Schwandorf', 'Letzter Artikel', 'Amberg', 'Letzter Artikel']
    df_final = df_final.fillna('-')
    return df_final

@flow(name='complete', log_prints=True)
def main_flow():
    #df = clean_data()
    df = 'test'
    
    # Load Google API credentials from GitHub secre
    credentials_json = json.loads(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(info=credentials_json)

    # Initialize the Google Sheets client
    client = gspread.authorize(credentials)
    
    # Define the target Google Sheets file and worksheet
    sheet_id = '17nUX78a09BQHKrPU8NrzynGVwwMEjeMcTS17ggKgzR8'
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
