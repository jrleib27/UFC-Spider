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

def time_keeper():
    time.sleep(random.randrange(1,3,1))

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

def most_recent_event_selector(bs):
    #could edit this to pull every single event on page then compare those results to those in the db that way I can avoid having to manually
    #run after every UFC event
    
    #adding the url for the most recent event on the homepage to a list to access and open
    url_list=[]

    event_object=bs.find('tbody').find_all('tr', class_="b-statistics__table-row")[1]
    event_url=event_object.find_all('a')
    for i in event_url:
        url_list.append(i['href'])

    url=url_list[0]
    #getting just the unique portion of the url to compare to the primary key in the database to avoid duplicates
    primary_key_comparer=url.split("/")[-1]

    return url, primary_key_comparer

def sql_puller(cur,conn):
    df=pd.read_sql(
        """
        SELECT event_url
        FROM events
        """, conn
    )
    
    url_list = df.event_url.to_list()
    
    return url_list
    
def text_fixer(text):
    #UFC adds in space and return statements that make the response unclean
    #this cleans it an returns the figure
    clean_list=[]
    for string in text:
        if (string !=''):
            clean_list.append(string)
    clean_text=clean_list[-1]
    return clean_text

def db_writer(stat_list,cur,conn):
    title = stat_list[0]
    date = stat_list[1]
    location = stat_list[2]
    attendance = stat_list[3]
    url = stat_list[4]

    query="INSERT INTO events (event_title,event_date,event_location,event_attendance,event_url)" \
        'VALUES(%s,%s,%s,%s,%s)'
    args=(title,date,location,attendance,url)
    cur.execute(query,args)

    cur.connection.commit()

def title_puller(bs):
    try:
        event_title = bs.find('span',class_="b-content__title-highlight").get_text().replace("\n", "").split("  ")
        title=text_fixer(event_title)
    except AttributeError as e:
        title = None
    except IndentationError as i:
        title = None
    return title

def date_puller(bs):
    try:
        unclean_date=bs.find_all('li',class_='b-list__box-list-item')[0].get_text().replace("\n", "").split("  ")
        date=text_fixer(unclean_date)
    except AttributeError as a:
        date=None
    except IndexError as i:
        date=None
    return date

def location_puller(bs):
    try:
        unclean_location=bs.find_all('li',class_='b-list__box-list-item')[1].get_text().replace("\n", "").split("  ")
        location=text_fixer(unclean_location)
    except AttributeError as a:
        location=None
    except IndexError as i:
        location=None
    return location 

def attendance_puller(bs):
    try:
        unclean_attendance=bs.find_all('li',class_='b-list__box-list-item')[2].get_text().replace("\n", "").split("  ")
        attendance=text_fixer(unclean_attendance)
    except AttributeError as a:
        attendance=None
    except IndexError as i:
        attendance=None
    return attendance

def event_scraper(pulled_url, primary_key_comparer,cur,conn):
    url_list=sql_puller(cur,conn)
    bs, event_URL=URL_opener_and_bs_creator(pulled_url)

    title=title_puller(bs)
    date=date_puller(bs)
    location=location_puller(bs)
    attendance=attendance_puller(bs)
        
    #writing to database
    stat_list=[title,date,location,attendance,primary_key_comparer]

    db_writer(stat_list,cur,conn)
    time_keeper()
    return title