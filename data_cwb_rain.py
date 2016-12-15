# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 10:01:59 2016

@author: wpk
"""
from datetime import datetime, timedelta
import requests
#import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
from pandas.io import sql
#from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb


def scrapecwb():
    # Scrape the HTML at the url
    baseurl = "http://www.cwb.gov.tw/V7/observe/rainfall/Rain_Hr/22.htm"
    r = requests.get(baseurl)
    r.encoding = "utf-8"

    # Turn the HTML into a Beautiful Soup object
#    soup = BeautifulSoup(r.text, "html.parser")
    soup = BeautifulSoup(r.text, "lxml")
    #    soup.prettify()

#    code = r.encoding
#    soup = BeautifulSoup(r.text.encode(code).decode("utf-8"), "lxml")

    # print soup
    # Create an object of the first object that is class=Form00
    table = soup.find("table", {"class": "tablesorter"})
    # print table
    lo = []
    st = []
    titleid = []
    time = []
    rf = []

    # Find all the <tr> tag pairs, skip the first one, then for each.
    for row in table.find_all('tr')[1:]:
        # Create a variable of all the <td> tag pairs in each <tr> tag pair,
        col = row.find_all('td')
        # print len(col)
        for tds in range(2, 26):
            # print tds
            location = col[0].text
            lo.append(location)

            title = col[1].span["title"]
            station = title[5:]

            st.append(station)
            titleid.append(title[:5])
            # print col[tds].string
            now = datetime.now() - timedelta(hours=tds - 1)
            stntime = now.strftime("%Y-%m-%d %H:00")
            time.append(stntime)

            if col[tds].string == "X":
                rf.append("null")
            elif col[tds].string == "-":
                rf.append("0")
            elif float(col[tds].string) > 0:
                rf.append(float(col[tds].string.strip()))

    # Create a variable of the value of the columns
    coldata = {'id': titleid, 'time': time, 'rf': rf}
    stdata = {'lo': lo, 'st': st, 'id': titleid, 'time': time, 'rf': rf}
    # print coldata
    # Create a dataframe from the columns variable
    df_cwb = pd.DataFrame(coldata, columns=['id', 'time', 'rf'])
    stid_rain = pd.DataFrame(
        stdata, columns=['lo', 'st', 'id']).drop_duplicates()
    return df_cwb, stid_rain
#     print df_cwb


def exinscwb2db():
    now = datetime.now()
    nowtime = now.strftime("%Y-%m-%d %H:00")
    # print nowtime
    last24 = datetime.now() - timedelta(hours=24)
    last24time = last24.strftime("%Y-%m-%d %H:00")
    # print last24time
    selectsqlquery = "SELECT *  FROM cwb_rainfall_data_temp WHERE " \
        "time >= '" + nowtime + "' and time <= '" + last24time + "'"
    deletesqlquery = "DELETE FROM cwb_rainfall_data_temp WHERE " \
        "time >= '" + last24time + "' and time <= '" + nowtime + "'"
    # print deletesqlquery
    # db = create_engine('mysql://ncree_gcclab_adm:ncree_gcclab888@127.0.0.1/ggclab')
#    db = create_engine('mysql://root:FCJ,51w48-I0yh@127.0.0.1/ggclab')
    db = MySQLdb.connect(host="140.109.80.146",
                         user="****",
                         passwd="****",
                         use_unicode=True,
                         charset="utf8",
                         db="IES")
#    db.encoding = "utf-8"

    df_mysql = pd.read_sql_query(selectsqlquery, db)
    [df_cwb, stid_rain] = scrapecwb()
    frames = [df_mysql, df_cwb]
    df_result = pd.concat(frames, ignore_index=True,
                          verify_integrity=True, names='time')

    # print df_result
    df_result = df_result.groupby(['id', 'time', 'rf']).size().reset_index()
    del df_result[0]
    df_result = df_result.ix[:, 0:]
    sql.execute(deletesqlquery, db)
    sql.to_sql(df_result, con=db,
               name='cwb_rainfall_data',
               index=None,
               if_exists='append',
               flavor='mysql')

    sql.to_sql(stid_rain, con=db,
               name='cwb_rainfall_stid',
               index=None,
               if_exists='replace',
               flavor='mysql')

    print("writing cwb_rainfall data to DB.....")
    print("writing cwb_rainfall_stid to DB.....")
#[df_cwb,stid_rain] = scrapecwb()
exinscwb2db()
