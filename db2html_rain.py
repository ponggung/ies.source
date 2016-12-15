# -*- coding: utf-8 -*-
"""
Created on Fri Aug  5 15:23:15 2016

@author: wpk
"""
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import matplotlib.pyplot as plt
import time


def grep(st):

    db = MySQLdb.connect(host="140.109.80.146",
                         user="****",
                         passwd="****",
                         use_unicode=True,
                         charset="utf8",
                         db="IES")

    # 台北市陽明山 氣象站
    # 嘉義縣大林鎮 大林地震事務所
    # 東華大學
    # 屏東縣橫村鎮 墾丁雷達站

    gammaid = {"CCUG": "C0M76", "KTPG": "46759",
               "DHUG": "C0Z10", "YMSG": "46691"}
    select = "select * from `cwb_rainfall_data` where  `id`='" + \
        gammaid[st] + "'"
    df = pd.read_sql_query(select, db)
    return df


def writejs(st, df):
    filename = st + "_rain.js"
    with open(filename, "w") as f:
        f.write(df.to_json(orient='records'))
    f.close()
    print("writing rain data" + st)


def tolist(df):
    dt = df["time"]
    ts = dt.apply(lambda x: str(time.mktime(x.timetuple()) * 1000))
    ts = ts.tolist()
    rf = df["rf"].tolist()
    return ts, rf


def writeRain(var, time, count):
    filename = "Rain.js"
    with open(filename, "a") as f:
        f.write("var " + var + "= [" + '\n')
        for i, item in enumerate(time):
            line = "[" + time[i] + "," + \
                str(count[i]) + ',\n' + "]" + "," + '\n'
            f.write(line)
        f.write("  ]; \n")
        f.close()
    print("writing Rain " + var)


# Main code
st = ["YMSG", "CCUG", "DHUG", "KTPG"]


for station in st:
    df = grep(station)
    [ts, rf] = tolist(df)
#    writejs(station,df)
    varR = station + "_rain"
    writeRain(varR, ts, rf)
    plt.plot(df["time"], df["rf"])
