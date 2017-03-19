#encoding=utf-8
import sys
from imp import reload
reload(sys)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
sys.setdefaultencoding('utf-8')
'''
Created on 2017年3月10日

@author: libin_m
'''
 
        
def normal(df,col):

    mean = df.select(mean(col).alias("mean")).show().mean
    std = df.select(stddev(col).alias("std")).show().std
 
    
    if std<0.0000000001 and std>-0.00000001:
        raise Exception("分母不能为零")
    
    df.select(((col - mean)/std).alias("new"))
    df.drop(col)
    
def maxMin(df,col):
    max = df.select(max(col).alias("max")).collect().max
    min = df.select(min(col).alias("min")).collect().min
 
    denominator = max - min 
    if denominator < 0.0000000001 and denominator >-0.00000001:
        raise Exception("分母不能为零")
    
    df.select(((col - min)/denominator).alias("new"))
    df.drop(col)
    
        
def encode(df,col):
    
    df.createOrReplaceTempView("tab")
    
    temp_df = df.select(col)
    
    # window function
    window = Window.partitionBy(col).orderBy(col)
    mark = temp_df.withColumn("rank",rank.over(window))
    result = df.join(mark,col,"left_outer").drop([df[col],mark[col]])
    return result


# OneHotEncoder


    
    
    
        

        
            
            
             
            
            
        
            
