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

def URL_processor(URLs):
    URL1=URLs[0].split('/')[-1]
    URL2=URLs[1].split('/')[-1]

    URL_list=[URL1,URL2]

    return URL_list

def SQL_pull_and_url_processor(fighter_URL_list,cur,conn):
    #this function pulls in all the fighter URLs already in the database so that I can make sure that no duplicates are added
    url_frame=pd.read_sql(
        '''
        SELECT fighter_url
        FROM fighters; 
        ''', conn
    )

    already_in_db_list=url_frame.fighter_url.to_list()

    URLs_to_process=[]
    URL_to_pass_list=[]

    for i in fighter_URL_list:
        URL_list=URL_processor(i)
        for i in URL_list:
            if i in already_in_db_list:
                pass
            else:
                URLs_to_process.append(i)

    for i in URLs_to_process:
        URL = 'http://ufcstats.com/fighter-details/'+str(i)
        URL_to_pass_list.append(URL)

    return URL_to_pass_list

def name_puller(bs):
    try:
        unprocessed_name = bs.find('h2').find('span').get_text().replace("\n", "").split("  ")
        name=text_fixer(unprocessed_name)
    except AttributeError as a:
        name = None
    except IndexError as e:
        name = None
    return name

def stat_box_puller(bs,URL):
    try:
        #getting name
        name=name_puller(bs)
        
        #getting URL
        Url=URL.split('/')[-1]

        #getting stats
        unprocessed_box=bs.find_all('ul', class_='b-list__box-list')[0]
        
        height=height_puller(unprocessed_box)
        reach = reach_puller(unprocessed_box)
        stance=stance_puller(unprocessed_box)
        DOB=DOB_puller(unprocessed_box)
        #working on a solution to this but for now leaving it Null
        gender=None

        stat_list=[name,height,reach,stance,gender,DOB,Url]
    except AttributeError as a:
        stat_box = None
    except IndexError as e:
        stat_box = None
    return stat_list

def height_feet_fixer(series,column):
    series=series.astype(str)
    split_height_frame=series.str.split('\'')
    feet_list = []
    inches_list=[]
    for i in split_height_frame:
        feet_list.append(i[0])
        inches_list.append(i[1].split('"')[0])
    zipped_inches_feet = zip(feet_list,inches_list)
    frame_list = list(zipped_inches_feet)
    temp_frame = pd.DataFrame(frame_list, columns=['feet','inches'])
    temp_frame=temp_frame.astype(int)
    temp_frame[str(column)]=((temp_frame['feet']*12)+temp_frame['inches'])
    temp_frame=temp_frame[[str(column)]]
    return temp_frame

def reach_fixer(series, column):
    split_reach_frame=series.str.split('"')
    data_frame_list = []
    for i in split_reach_frame.to_list():
            data_frame_list.append(i[0])
    reach_frame = pd.DataFrame(data_frame_list, columns=[str(column)])
    reach_frame[column]=reach_frame[column].astype(int)
    return reach_frame

def height_puller(stat_box):
    try:
        height_unprocessed=stat_box.find_all('li',class_="b-list__box-list-item b-list__box-list-item_type_block")[0].get_text().replace("\n", "").split("  ")
        height_list=text_fixer(height_unprocessed)
        
        #turning height into inches
        #feet first
        feet=int(int(height_list.split('\'')[0])*12)
        #inches next
        inches=int((height_list.split('\'')[1]).split('"')[0])
        #putting it together
        height=str(feet+inches)
    except AttributeError as a:
        height=None
    except IndexError as e:
        height=None
    return height

def reach_puller(stat_box):
    try:
        reach_unprocessed=stat_box.find_all('li',class_="b-list__box-list-item b-list__box-list-item_type_block")[2].get_text().replace("\n", "").split("  ")
        reach_list=text_fixer(reach_unprocessed)
        reach = reach_list.split('"')[0]
    except AttributeError as a:
        reach=None
    except IndexError as e:
        reach=None
    return reach

def stance_puller(stat_box):
    try:
        stance_unprocessed=stat_box.find_all('li',class_="b-list__box-list-item b-list__box-list-item_type_block")[3].get_text().replace("\n", "").split("  ")
        stance=text_fixer(stance_unprocessed)
    except AttributeError as a:
        stance = None
    except IndexError as e:
        stance = None
    return stance

def DOB_puller(stat_box):
    try:
        DOB_unprocessed=stat_box.find_all('li',class_="b-list__box-list-item b-list__box-list-item_type_block")[4].get_text().replace("\n", "").split("  ")
        DOB=text_fixer(DOB_unprocessed)
    except AttributeError as a:
        DOB = None
    except IndexError as e:
        DOB = None
    return DOB

def database_Builder(personal_stat_list,cur,conn):
    print(personal_stat_list)
    name= personal_stat_list[0]
    height = personal_stat_list[1]
    reach = personal_stat_list[2]
    stance = personal_stat_list[3]
    date_of_birth = personal_stat_list[5]
    gender = personal_stat_list[4]
    url_for_frame = personal_stat_list[6]

    query="INSERT INTO fighters (fighter_name,fighter_height,fighter_reach,fighter_stance,fighter_gender,fighter_dob, fighter_url)" \
        'VALUES(%s,%s,%s,%s,%s,%s,%s)'
    args=(name,height,reach,stance,gender,date_of_birth,url_for_frame)
    cur.execute(query,args)

    cur.connection.commit()

def fighter_scraper(fighter_URL_list,cur,conn):
    URL_list=SQL_pull_and_url_processor(fighter_URL_list,cur,conn)
    for i in URL_list:
        bs, URL=URL_opener_and_bs_creator(i)
        stat_list=stat_box_puller(bs,URL)
        database_Builder(stat_list,cur,conn)
        time_keeper()