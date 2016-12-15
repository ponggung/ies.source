# -*- coding: utf-8 -*-
"""
Created on Fri Jul 15 16:22:54 2016
@author: wpk
download CWB_EQ event table , and update to DataBase
http://www.cwb.gov.tw/V7e/modules/MOD_EC_Home.htm
"""
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from html_table_parser import parser_functions as parse
import pandas as pd

from pandas.io import sql
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb




def parse_table(url):

    url = 'http://www.cwb.gov.tw/V7e/modules/MOD_EC_Home.htm'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
#    soup . prettify()
    table = soup.find('table')
    data = parse.make2d(table)
#    col = data[0]
    col = ['Number', 'DateTime', 'LAT', 'LON', 'Mag', 'Depth', 'Location', 'url']
    df = pd.DataFrame(data[1:], columns=col)
    return df



def eq2db():
    url = 'http://www.cwb.gov.tw/V7e/modules/MOD_EC_Home.htm'
    #url = "http://www.cwb.gov.tw/V7/earthquake/rtd_eq.htm"
    df = parse_table(url)

    now = datetime.now()
    nowyear = now.strftime("%Y")
    eqtime = []
    for row in df["DateTime"]: eqtime .append( nowyear +'/'+ row )
    df2 = df
    df2["DateTime"] = eqtime

    db = MySQLdb.connect(host = "140.109.80.146" ,
                         user = "****" ,
                         passwd = "****",
                         db="IES")


    sql.to_sql(df2, con=db, name='cwb_eq',
               if_exists='append',
               index = None,
               flavor='mysql')

    print("writing data to DB.....")




    Igonre_same = "CREATE TABLE tmp AS SELECT DISTINCT * FROM cwb_eq;"\
                  "DROP TABLE cwb_eq;"\
                  "RENAME TABLE tmp TO cwb_eq;"

#    Alter = "ALTER IGNORE TABLE cwb_eq"\
#             "ADD UNIQUE INDEX (LAT, LON, Mag, Depth);"
    sql.execute(Igonre_same, db)



eq2db()
