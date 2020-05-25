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
from db import db_startup
from event_scraper import event_scraper
import random


def time_keeper():
    time.sleep(random.randrange(1,3,1))

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

def sql_puller(cur,conn):
    df=pd.read_sql(
        """
        SELECT fight_url
        FROM fights
        """, conn
    )
    
    url_list = df.fight_url.to_list()
    
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

def name_puller(bs):
    
    fighter_list=[]
    final_fighter_list=[]
    try:
        fighters=bs.find_all('td',class_="b-fight-details__table-col")
        fighter_list.append(fighters[1].get_text().replace("\n", "").split("  "))

        for string in fighter_list[0]:
            if (string != ""):
                final_fighter_list.append(string)
                
        winning_fighter=final_fighter_list[0]
        losing_fighter=final_fighter_list[1]
    except AttributeError as e:
        winning_fighter = None
        losing_fighter = None
    except IndexError as e:
        winning_fighter = None
        losing_fighter = None
    return winning_fighter,losing_fighter

def weight_class_puller(bs):
    try:
        class_list=[]
        weight_class_unclean=bs.find_all('td',class_="b-fight-details__table-col")[6].get_text().replace("\n", "").split("  ")
        
        weight_class = text_fixer(weight_class_unclean)

    except AttributeError as e:
        weight_class = None
    except IndexError as e:
        weight_class = None
    return weight_class

def method_puller(bs):
    try:
        method_list=[]
        method=bs.find_all('td',class_="b-fight-details__table-col")[7].get_text().replace("\n", "").split("  ")
        
        for string in method:
            if (string != ''):
                method_list.append(string)
        
        method=method_list[0]
        specific_method=method_list[1]

    except AttributeError as e:
        method = None
        specific_method = None
    except IndexError as e:
        method = None
        specific_method = None
            
    return method,specific_method

def round_time_puller(bs):
    try:
        round_unclean=bs.find_all('td',class_="b-fight-details__table-col")[8].get_text().replace("\n", "").split("  ")

        final_round = text_fixer(round_unclean)

    except AttributeError as e:
        final_round = None
    except IndexError as e:
        final_round = None
    
    try:
        final_time_unclean=bs.find_all('td',class_="b-fight-details__table-col")[9].get_text().replace("\n", "").split("  ")
        
        final_time = text_fixer(final_time_unclean)

    except AttributeError as e:
        final_time = None
    except IndexError as e:
        final_time = None
    
    return final_round, final_time

def url_puller(bs):
    url=bs.get('data-link')
    url = url.split('/')[-1]
    return url

def db_pusher(stat_list,cur,conn):
    print(stat_list)
    event = stat_list[0]
    winning_fighter = stat_list[1]
    losing_fighter = stat_list[2]
    weight=stat_list[3]
    method=stat_list[4]
    specific=stat_list[5]
    fightround=stat_list[6]
    fighttime=stat_list[7]
    url=stat_list[8]

    query="INSERT INTO fights (fight_title,winning_fighter_name,losing_fighter_name,weight_class,method_of_victory,specific_victory_details,ending_round,ending_time,fight_url)" \
        'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    args=(event,winning_fighter,losing_fighter,weight,method,specific,fightround,fighttime,url)

    cur.execute(query,args)

    cur.connection.commit()

def fighter_and_round_url_puller(soup):
    #initiating lists
    list_of_urls=[]
    fighter_URL_list=[]
    round_URL_list=[]

    round_URL_list.append(soup['data-link'])
    
    for a in soup.find_all('a',class_="b-link b-link_style_black"):
            fighter_URL_list.append(a['href'])

    return fighter_URL_list,round_URL_list

def fights_scraper(pulled_url,cur,conn,event_name):
    bs, event_URL=URL_opener_and_bs_creator(pulled_url)
    url_list=sql_puller(cur,conn)
    
    fighter_URL_list=[]
    round_URL_list=[]
    time_keeper()
    
    for i in bs.find('tbody',class_='b-fight-details__table-body').find_all('tr',class_="b-fight-details__table-row"):
        #stat puller
        winner,loser=name_puller(i)
        weight_class=weight_class_puller(i)
        method,specific_method=method_puller(i)
        final_round,final_time=round_time_puller(i)
        fight_url=url_puller(i)
        
        #URL pull
        fighter_URL,round_URL=fighter_and_round_url_puller(i)
        fighter_URL_list.append(fighter_URL)
        round_URL_list.append(round_URL)

        #putting it together and uploading to db
        fight_table_list=[event_name,winner,loser,weight_class,method,specific_method,final_round,final_time,fight_url]
        db_pusher(fight_table_list,cur,conn)

    return fighter_URL_list,round_URL_list