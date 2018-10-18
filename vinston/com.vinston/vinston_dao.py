# -*- coding:utf-8 -*-
import logging
import datetime
import configparser
import math
import time
import sys
import platform
import os
import requests
import ast

# from pymongo import MongoClient

"""
    author:lees
    date:2018-8-20
    name:数据访问对象接口
    usage:定义操作数据库接口api，子类继承该接口实现各自方法
"""
class Dao():
    def __init__(self):
        pass
    def connect(self,dbtype,ip,usr,pwd):
        """
            定义连接数据库的方法
        """
        pass
    def update(self):
        """
            定义更新数据库的方法
        """
        pass
    def insert(self):
        """
            定义更新数据库的方法
        """
        pass
    def query(self):
        """
            定义查询数据库(DDL)的方法
        """
        pass

    def rollback(self):
        """
            定义数据库数据回滚的方法
            具体是指操作步骤中的某个步骤没有完成，则整个操作撤销
        """
        pass
    def close(self):
        """
            定义关闭数据库的方法
        """
        pass
    def destroy(self):
        """
            定义关闭数据库之后的回调函数(非必选)
        """
        pass
