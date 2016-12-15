# -*- coding: utf-8 -*-
# @Author: WPK
# @Date:   2016-09-12 14:43:07
# @Last Modified by:   WPK
# @Last Modified time: 2016-09-30 11:26:14
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 12:43:00 2016

@author: wpk
"""



import schedule
import time
import os

py = "/home/alpha/anaconda3/bin/python "

workpath = os.getcwd()
log = " &> " + os.getcwd() + "log"


def test():
    print("I'm working")

def eq():
#    print("cwb eq. ...")
    cmd = py + workpath + "/data_cwb_eq.py" + log
    os.system(cmd)


def rain():
#    print("cwb rain.....")
    cmd = py + workpath + "/data_cwb_rain.py" +log
    os.system(cmd)
def db_rain():
#    print("db_rain....")
    cmd = py + workpath + "/db2html_rain.py" + log
    os.system(cmd)
def typhoon():
#    print("db_rain....")
    cmd = py + workpath + "/data_cwb_typhoon.py" + log
    os.system(cmd)

#sample
#schedule.every(1).minutes.do(job)
#schedule.every(1).hour.do(job)
#schedule.every().day.at("12:49").do(job)


schedule.every(1).hour.do(rain)
schedule.every(1).hour.do(eq)
schedule.every(1).hour.do(db_rain)
schedule.every(1).hour.do(typhoon)

while 1:
    schedule.run_pending()
    time.sleep(1)
+