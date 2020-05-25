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
import random

def URL_fixer(URL):
    URL=str(URL)
    small_URL = URL.split('/')[-1]
    final_URL = str('http://ufcstats.com/fight-details/'+str(small_URL))
    return final_URL

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

def text_fixer(text):
    #UFC adds in space and return statements that make the response unclean
    #this cleans it an returns the figure
    clean_list=[]
    for string in text:
        if (string !=''):
            clean_list.append(string)
    clean_text=clean_list[-1]
    return clean_text

def fight_id_puller(event_name,bs,cur,conn):
    try:
        #getting winners name
        winner = bs.find('i',class_="b-fight-details__person-status b-fight-details__person-status_style_green").parent.find('a').get_text().replace("\n", "").split("  ")
        winner=winner[0].rstrip()

        #getting fight id
        query=('''
            SELECT fight_id
            FROM fights
            WHERE fight_title='{}' AND winning_fighter_name='{}'
            '''.format(str(event_name),str(winner)))

        fight_id_frame = pd.read_sql_query(query,conn)

        fight_id = fight_id_frame.fight_id.to_list()
        fight_id=str(fight_id[0])
    except AttributeError as a:
        pass

    return fight_id

def ref_and_round_scraper_individual(bs,passed_list,cur,conn):
    fight_id = passed_list[0]
    
    ###getting total rounds
    try:
        rounds_number=bs.find('i',class_="b-fight-details__label",text='''
          Time format:
        ''').parent.get_text().replace("\n", "").split("  ")
                
        number_of_rounds=text_fixer(rounds_number)

        #so it is only a number
        rounds=number_of_rounds.split(' ')[0]
    except IndexError:
        rounds=None
    except AttributeError:
        rounds=None
    
    ###getting referee name
    try:        
        referee_name=bs.find('i',class_="b-fight-details__label",text='''
          Referee:
        ''').parent.find('span',class_="").get_text().replace("\n", "").split("  ")

        referee=text_fixer(referee_name)
    except IndexError:
        referee=None
    except AttributeError:
        referee=None

    return rounds,referee

def ref_and_round_db(rounds,referee,fight_id,cur,conn):
    try:
        fight_id=str(fight_id)
        rounds=str(rounds)
        referee=str(referee)
        print(rounds,referee,fight_id)
        QUERY=(
        "INSERT INTO ref_and_rounds (fight_id,number_of_rounds,referee)"\
                'VALUES(%s,%s,%s)'
        )
        args=(fight_id,rounds,referee)

        cur.execute(QUERY,args)
        cur.connection.commit()
    except:
        pass

def ref_and_round_scraper(bs,event_name,cur,conn):
        fight_id=fight_id_puller(event_name,bs,cur,conn)
        rounds,referee=ref_and_round_scraper_individual(bs,fight_id,cur,conn)
        ref_and_round_db(rounds,referee,fight_id,cur,conn)
        time_keeper()