import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from prefect import flow, task
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

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



@flow(name='combine_dfs', log_prints=True)
def combine_dfs():
    rgb = extract_urls('rgb')
    print('rgb done')
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


@flow(name='wird schon', log_prints=True)
def main_flow():
    df = combine_dfs()
    df_final = pd.concat([df[0], df[1], df[2], df[3], df[4], df[5]], axis=1)
    df_final.fillna('-')
    df_final.columns = ['Regensburg', 'Letzter Artikel', 'Cham', 'Letzter Artikel', 
                        'Neumarkt', 'Letzter Artikel', 'Kelheim', 'Letzter Artikel',
                        'Schwandorf', 'Letzter Artikel', 'Amberg', 'Letzter Artikel']
    df_final = df_final.fillna('-')
    df_final.to_excel('gemeindeabdeckung_mz.xlsx')
    

if __name__ == '__main__':
    main_flow()