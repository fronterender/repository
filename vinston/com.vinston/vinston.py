# -*- coding:utf-8 -*-
from Transform import Transform
from QueryData import QueryData
import configparser
import pymysql
import  time
import writelogging
import sys
from Transform_tmk import Transform_tmk
cp = configparser.ConfigParser()
cp.read("config.cfg",encoding="utf-8")

def run():
    transform = Transform()
    transform.extract()
    transform.transform()
    transform.load()
    print ("完成市场主题处理")
    transform_tmk = Transform_tmk()
    transform_tmk.extract()
    transform_tmk.transform()
    transform_tmk.load()
    print ("完成TMK主题处理")
run()
