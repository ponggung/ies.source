# -*- coding: utf-8 -*-
"""
# @Author: WPK
# @Date:   2016-09-20 16:59:18
# @Last Modified by:   WPK
# @Last Modified time: 2016-12-14 14:06:20
"""

from datetime import datetime
from selenium import webdriver
import time
from bs4 import BeautifulSoup
from html_table_parser import parser_functions as parse
import pandas as pd
from pandas.io import sql
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb



def parse_table():
    driver = webdriver.PhantomJS()
    driver.set_window_size(1120, 550)
    # time.sleep(10)
    driver.get("http://rdc28.cwb.gov.tw/TDB/ntdb/pageControl/ty_warning")
    r = driver.find_element_by_class_name("content2").click()

    soup = BeautifulSoup(driver.page_source, 'lxml', from_encoding="utf-8")
    table = soup.find('table')
    data = parse.make2d(table)
    col = ['year', 'num', 'Chinese_name', 'English_name', 'path__case',
           'alarn_period', 'strength', 'lowest_hPa', 'highest_wind_speed', '7grade_wind_radius', '10grade_wind_radius', 'alarn_number']

    df = pd.DataFrame(data[1:], columns=col)
    return df


def typ2db(df):
    db = MySQLdb.connect(host="140.109.80.146",
                         user="****",
                         passwd="****",
                         use_unicode=True,
                         charset="utf8",
                         db="IES")

    sql.to_sql(df, con=db, name='cwb_typhoon',
               if_exists='append',
               index=None,
               flavor='mysql')

    print("writing data to DB.....")

    Igonre_same = "CREATE TABLE tmp AS SELECT DISTINCT * FROM cwb_typhoon;"\
                  "DROP TABLE cwb_typhoon;"\
                  "RENAME TABLE tmp TO cwb_typhoon;"

#    Alter = "ALTER IGNORE TABLE cwb_typhoon"\
#             "ADD UNIQUE INDEX (LAT, LON, Mag, Depth);"
    sql.execute(Igonre_same, db)


def main():
    df = parse_table()
    typ2db(df)

if __name__ == "__main__":
    main()

# print(data)

# for rows in data[3:]:
#     print(rows)
