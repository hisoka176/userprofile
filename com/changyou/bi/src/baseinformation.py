#encoding=utf-8
import sys
from imp import reload
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession

'''
Created on 2017年3月10日

@author: libin_m
'''


      
#### 得到各个属性的NULL覆盖率  
def na_string(df):
    
    string = ''
    
    columns = df.columns
    for col in columns:
        na_count = df.where(df.col.isNull).count()
        string += "| "+col+" | "+str(na_count) + '|\n'
    
    return string

#### 各个属性的分布
def attri_information(ssc,df):
    
    df.createOrRaplaceTempView('tab')
    columns = df.columns
    for col in df.columns:
        sql_string = "select "+col+",count(*) from tab group by " + col +"order by col"
        ssc.sql(sql_string)
        
    
    
             
            
            
            
        
