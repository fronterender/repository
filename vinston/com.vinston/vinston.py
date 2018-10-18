# -*- coding:utf-8 -*-
from Transform import Transform
from TransformTmk import TransformTmk
from TransformContract import TransformContract
from TransformVisit import TransformVisit
from QueryData import QueryData
from threading import Timer
import configparser
import pymysql
import time
import writelogging
import sys
import datetime

cp = configparser.ConfigParser()
cp.read("config.cfg",encoding="utf-8")
def market_etl():
    transform = Transform()
    transform.extract()
    transform.transform()
    transform.load()
    print ("完成市场主题处理")
    print ()
def tmk_etl():
    transformTmk = TransformTmk()
    transformTmk.extract()
    transformTmk.transform()
    transformTmk.load()
    print ("完成TMK主题处理")
    print ()
def contract_etl():
    transformContract = TransformContract()
    transformContract.extract()
    transformContract.transform()
    transformContract.load()
    print ("完成合同主题处理")
    print ()
def visit_etl():
    transformVisit = TransformVisit()
    transformVisit.extract()
    transformVisit.transform()
    transformVisit.load()
    print ("完成到访主题处理")
def run():
    start = datetime.datetime.now()
    market_etl()
    tmk_etl()
    contract_etl()
    visit_etl()
    end = datetime.datetime.now()
    print ("累计历时  %d 秒"%(end-start).total_seconds())
    writelogging.logger.info("任务执行完毕,耗时 %d 秒"%((end-start).total_seconds()))
def vinston():
    run()
vinston()
