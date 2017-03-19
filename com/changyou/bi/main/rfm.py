#encoding=utf-8
import sys
from imp import reload
reload(sys)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.mllib import import clustering
sys.setdefaultencoding('utf-8')
'''
Created on 2017年3月10日

@author: libin_m
'''
tablePath = ''

#get spark_sql_context
ssc = SparkSession.builder.getOrCreate()

