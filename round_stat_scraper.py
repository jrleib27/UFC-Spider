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
from ref_and_round_scraper import ref_and_round_scraper

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

def round_totals_db(data_list,passed_list,round_num,cur,conn):
    try:
        #passed list = fight_id, event_id, url
        #fight_id
        fight_id = passed_list[0]
        #event_id
        event_id = passed_list[1]
        #UFCstats.com adds space to end of the fighters name for no reason. below just removes it
        fighter_name_list=data_list[0]
        fighter_name_with_space=fighter_name_list[0]
        fighter_name_with_two_space_int=len(fighter_name_with_space)+1
        fighter_name_with_two_space_str=fighter_name_with_space.ljust(fighter_name_with_two_space_int)
        fighter_name=fighter_name_with_two_space_str.split('  ')[0]
        #knockdowns
        knockdown_list=data_list[1]
        knockdowns=knockdown_list[0]
        #total strikes
        total_strikes_list=data_list[4]
        total_strikes = total_strikes_list[0]
        total_strikes_landed=total_strikes.split(' ')[0]
        total_strikes_attempted=total_strikes.split(' ')[2]
        #takedowns
        takedowns_list=data_list[5]
        takedowns=takedowns_list[0]
        takedowns_landed=takedowns.split(' ')[0]
        takedowns_attempted=takedowns.split(' ')[2]
        #sub attempts
        subs_list=data_list[7]
        subs=subs_list[0]
        #guard passes
        guard_list = data_list[8]
        guard_passes=guard_list[0]
        #reversals
        rev_list = data_list[9]
        rev = rev_list[0]

        QUERY = "INSERT INTO round_totals (unique_fight_id,unique_event_id,round_number,fighter_name,knockdowns,total_strikes_landed,total_strikes_attempted,takedowns,takedowns_attempted,submission_attempts,guard_passes,reversals)" \
            'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        args = (fight_id,event_id,round_num,fighter_name,knockdowns,total_strikes_landed,total_strikes_attempted,takedowns_landed,takedowns_attempted,subs,guard_passes,rev)
        print(args)
        cur.execute(QUERY,args)

        cur.connection.commit()
    except AttributeError:
        print('not uploaded round')
        pass

def sig_strikes_db(data_list,passed_list,round_num,cur,conn):
    try:
        #passed list = fight_id, event_id, url
        #unique_fight_id
        fight_id = passed_list[0]
        #unique_event_id
        event_id = passed_list[1]
        #round_number
        round_num=round_num
        #fighter_name
        fighter_name_list=data_list[0]
        fighter_name_with_space=fighter_name_list[0]
        fighter_name_with_two_space_int=len(fighter_name_with_space)+1
        fighter_name_with_two_space_str=fighter_name_with_space.ljust(fighter_name_with_two_space_int)
        fighter_name=fighter_name_with_two_space_str.split('  ')[0] 
        #significant_strikes_head_landed
        significant_strikes_head_landed_list=data_list[3]
        significant_strikes_head_string=significant_strikes_head_landed_list[0]
        significant_strikes_head_landed=(significant_strikes_head_string.split(' ')[0])
        #significant_strikes_head_attempted
        significant_strikes_head_attempted=(significant_strikes_head_string.split(' ')[2])
        #significant_strikes_body_landed
        significant_strikes_body_landed_list=data_list[4]
        significant_strikes_body_string=significant_strikes_body_landed_list[0]
        significant_strikes_body_landed=(significant_strikes_body_string.split(' ')[0])
        #significant_strikes_body_attempted
        significant_strikes_body_attempted=(significant_strikes_body_string.split(' ')[2])
        #significant_strikes_leg_landed
        significant_strikes_leg_list=data_list[5]
        significant_strikes_leg_string=significant_strikes_leg_list[0]
        significant_strikes_leg_landed=(significant_strikes_leg_string.split(' ')[0])
        #significant_strikes_leg_attempted
        significant_strikes_leg_attempted=(significant_strikes_leg_string.split(' ')[2])
        #significant_strikes_standing_landed
        significant_strikes_standing_list=data_list[6]
        significant_strikes_standing_string=significant_strikes_standing_list[0]
        significant_strikes_standing_landed=(significant_strikes_standing_string.split(' ')[0])
        #significant_strikes_standing_attempted
        significant_strikes_standing_attempted=(significant_strikes_standing_string.split(' ')[2])
        #significant_strikes_clinch_landed
        significant_strikes_clinch_list=data_list[7]
        significant_strikes_clinch_string=significant_strikes_clinch_list[0]
        significant_strikes_clinch_landed=(significant_strikes_clinch_string.split(' ')[0])
        #significant_strikes_clinch_attempted
        significant_strikes_clinch_attempted=(significant_strikes_clinch_string.split(' ')[2])
        #significant_strikes_ground_landed
        significant_strikes_ground_list=data_list[8]
        significant_strikes_ground_string=significant_strikes_ground_list[0]
        significant_strikes_ground_landed=(significant_strikes_ground_string.split(' ')[0])
        #significant_strikes_ground_attempted
        significant_strikes_ground_attempted=(significant_strikes_ground_string.split(' ')[2])
        
        QUERY= "INSERT INTO sig_strike_rounds (unique_fight_id,unique_event_id,round_number,fighter_name,significant_strikes_head_landed,significant_strikes_head_attempted,significant_strikes_body_landed,significant_strikes_body_attempted,significant_strikes_leg_landed,significant_strikes_leg_attempted,significant_strikes_standing_landed,significant_strikes_standing_attempted,significant_strikes_clinch_landed,significant_strikes_clinch_attempted,significant_strikes_ground_landed,significant_strikes_ground_attempted)" \
            'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

        args=(fight_id,event_id,round_num,fighter_name,significant_strikes_head_landed,significant_strikes_head_attempted,significant_strikes_body_landed,significant_strikes_body_attempted,significant_strikes_leg_landed,significant_strikes_leg_attempted,significant_strikes_standing_landed,significant_strikes_standing_attempted,significant_strikes_clinch_landed,significant_strikes_clinch_attempted,significant_strikes_ground_landed,significant_strikes_ground_attempted)
        print(args)

        cur.execute(QUERY,args)
        cur.connection.commit()
    except:
        print('not uploaded sig strike')
        pass

def event_puller(event_name,curr,conn):
    #getting fight id
    query=('''
        SELECT event_id
        FROM events
        WHERE event_title='{}'
        '''.format(str(event_name)))

    event_frame = pd.read_sql_query(query,conn)

    event_id = event_frame.event_id.to_list()
    event_id=str(event_id[0])

    return event_id

def round_scraper(bs,url,passed_list,cur,conn):
    fight_id = passed_list[0]
    event_id = passed_list[1]

    #accessing the totals rows
   
    table_list=[]

    for i in bs.find_all('table',class_="b-fight-details__table js-fight-table"):
        table_list.append(i)
    
    #pulling the total stats table
    round_num = 1
    try:
        for i in bs.find('table',class_="b-fight-details__table js-fight-table").find('tbody').find_all('tr',class_="b-fight-details__table-row"):
            total_stat_puller(i,round_num,passed_list,cur,conn)
            round_num+=1
    except AttributeError:
        fighter_1_master_stat_list=[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,]
        fighter_2_master_stat_list=[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,]
        round_totals_db(fighter_1_master_stat_list,passed_list,round_num,cur,conn)
        round_totals_db(fighter_2_master_stat_list,passed_list,round_num,cur,conn)


    
    #resetting round num and then pulling the sig strike table
    round_num = 1
    try:
        for i in table_list[1].find('tbody').find_all('tr',class_="b-fight-details__table-row"):
            sig_strike_puller(i,round_num,passed_list,cur,conn)
            round_num+=1
    except AttributeError:
        fighter_1_master_sig_strikes_list=[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,]
        fighter_2_master_sig_strikes_list=[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,]
        sig_strikes_db(fighter_1_master_sig_strikes_list,passed_list,round_num,cur,conn)
        sig_strikes_db(fighter_2_master_sig_strikes_list,passed_list,round_num,cur,conn)
    
def total_stat_puller(i,round_num,passed_list,cur,conn):
    try:
        fighter_1_master_stat_list=[]
        fighter_2_master_stat_list=[]
        for a in i.find_all('td', class_="b-fight-details__table-col"):
            stat_list=[]
            fighter_1_cleaned_stat_list=[]
            fighter_2_cleaned_stat_list=[]
            for b in a.find_all('p',class_="b-fight-details__table-text"):
                stat_list.append(b.get_text().replace("\n", "").split("  "))
            
            fighter_1=stat_list[0]
            for i in fighter_1:
                if (i != ''):
                    fighter_1_cleaned_stat_list.append(i)

            fighter_2=stat_list[1]
            for i in fighter_2:
                if (i != ''):
                    fighter_2_cleaned_stat_list.append(i)
            
            fighter_1_master_stat_list.append(fighter_1_cleaned_stat_list)
            fighter_2_master_stat_list.append(fighter_2_cleaned_stat_list)
        
        round_totals_db(fighter_1_master_stat_list,passed_list,round_num,cur,conn)
        round_totals_db(fighter_2_master_stat_list,passed_list,round_num,cur,conn)
    except AttributeError:
        fighter_1_master_stat_list=[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,]
        fighter_2_master_stat_list=[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,]
        round_totals_db(fighter_1_master_stat_list,passed_list,round_num,cur,conn)
        round_totals_db(fighter_2_master_stat_list,passed_list,round_num,cur,conn)

def sig_strike_puller(i,round_num,passed_list,cur,conn):
    try:
        fighter_1_master_sig_strikes_list = []
        fighter_2_master_sig_strikes_list = []

        for a in i.find_all('td', class_="b-fight-details__table-col"):
            stat_list=[]
            fighter_1_cleaned_stat_list=[]
            fighter_2_cleaned_stat_list=[]
            for b in a.find_all('p',class_="b-fight-details__table-text"):
                stat_list.append(b.get_text().replace("\n", "").split("  "))
            
            fighter_1=stat_list[0]
            for i in fighter_1:
                if (i != ''):
                    fighter_1_cleaned_stat_list.append(i)

            fighter_2=stat_list[1]
            for i in fighter_2:
                if (i != ''):
                    fighter_2_cleaned_stat_list.append(i)
            
            fighter_1_master_sig_strikes_list.append(fighter_1_cleaned_stat_list)
            fighter_2_master_sig_strikes_list.append(fighter_2_cleaned_stat_list)  
        
        sig_strikes_db(fighter_1_master_sig_strikes_list,passed_list,round_num,cur,conn)
        sig_strikes_db(fighter_2_master_sig_strikes_list,passed_list,round_num,cur,conn)
    except AttributeError:
        fighter_1_master_sig_strikes_list=[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,]
        fighter_2_master_sig_strikes_list=[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,]
        sig_strikes_db(fighter_1_master_sig_strikes_list,passed_list,round_num,cur,conn)
        sig_strikes_db(fighter_2_master_sig_strikes_list,passed_list,round_num,cur,conn)

def list_maker(i):
    fight_id = int(i[0])
    event_id = int(i[1])
    url = str(i[2])
    fixed_list = [fight_id,event_id,url]
    return fixed_list

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
    except AttributeError:
        pass

    return fight_id

def round_stat_scraper(round_URL_list,event_name,cur,conn):
        for i in round_URL_list:
            try:
                #fixed_url=URL_fixer(i)
                fixed_url=i[0]
                bs, url=URL_opener_and_bs_creator(fixed_url)

                #ref and round scraper
                ref_and_round_scraper(bs,event_name,cur,conn)

                fight_id=fight_id_puller(event_name,bs,cur,conn)
                event_id=event_puller(event_name,cur,conn)
                passed_list=[fight_id,event_id]
                round_scraper(bs,url,passed_list,cur,conn)

                time_keeper()
            except UnboundLocalError:
                pass
    