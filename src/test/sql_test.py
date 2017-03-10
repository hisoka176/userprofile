#encoding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

'''
Created on 2017年3月6日

@author: libin_m
'''
USERPROFILE_MID_RECHARGE_DT0 = "/upbi/cyou/userprofile/mid_recharge_dt0"

from pyspark.sql import SQLContext
from pyspark  import SparkContext

sc = SparkContext()
mid_recharge_dt0 = sc.textFile(USERPROFILE_MID_RECHARGE_DT0)
mid_recharge_dt0.cache()
mid_recharge_dt0.count()

sqlContext = SQLContext(sc)
sqlContext.

sc.stop()