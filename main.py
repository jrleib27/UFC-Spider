from bs4 import BeautifulSoup
import pymysql
import requests
from urllib.request import urlopen
import pandas as pd
import numpy as np
import time
import datetime
import re
import os
import random
from db import db_startup
from event_scraper import event_scraper
from fights_scraper import fights_scraper
from fighter_scraper import fighter_scraper
from ref_and_round_scraper import ref_and_round_scraper
from round_stat_scraper import round_stat_scraper

def time_keeper():
    time.sleep(random.randrange(1,3,1))

def most_recent_event_selector(bs):
    
    #adding the url for all the events on the homepage to compare to events I already have
    url_list=[]

    event_object=bs.find('tbody').find_all('tr', class_="b-statistics__table-row")
    for i in event_object:
        event_url=i.find_all('a')
        for a in event_url:
            url_list.append(a['href'])

    return url_list

def primary_key_comparer_puller(pulled_url):
    #getting just the unique portion of the url to compare to the primary key in the database to avoid duplicates
    primary_key_comparer=pulled_url.split("/")[-1]
    return primary_key_comparer

def URL_opener_and_bs_creator(URL):
    try:
        html = requests.get(URL)
    except HTTPError as e:
        print(e)
        pass
    except URLError as e:
        print("Couldn't find server")
        pass
    data = html.text
    bs = BeautifulSoup(data, 'html.parser')
    return bs, URL

def text_fixer(text):
    #UFC adds in space and return statements that make the response unclean
    #this cleans it an returns the figure
    clean_list=[]
    for string in text:
        if (string !=''):
            clean_list.append(string)
    clean_text=clean_list[-1]
    return clean_text

def sql_puller(cur,conn):
    df=pd.read_sql(
        """
        SELECT event_url
        FROM events
        """, conn
    )
    
    url_list = df.event_url.to_list()
    
    return url_list

if __name__ == "__main__":
    cur,conn=db_startup()
    try:
        #main page scraper
        bs, excess_URL=URL_opener_and_bs_creator('http://ufcstats.com/statistics/events/completed')
        url_list=most_recent_event_selector(bs)
        for pulled_url in url_list:

            #pulling event primary key from URL
            primary_key_comparer=primary_key_comparer_puller(pulled_url)
            
            #pulling URLs already in db
            url_list=sql_puller(cur,conn)

            if primary_key_comparer in url_list:
                print(str(primary_key_comparer)+' is in list')
                pass
            
            else:
                #event table scraper (undo hastag when ready to go)
                event_name=event_scraper(pulled_url, primary_key_comparer,cur,conn)
                
                #fights table scraper
                fighter_URL_list,round_URL_list=fights_scraper(pulled_url,cur,conn,event_name)
                
                #fighter page scraper
                fighter_scraper(fighter_URL_list,cur,conn)

                #rounds scraper
                round_stat_scraper(round_URL_list,event_name,cur,conn)

    finally:
        cur.close()
        conn.close()