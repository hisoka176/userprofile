#encoding=utf-8
import sys
from imp import reload
reload(sys)
sys.setdefaultencoding('utf-8')
from selenium import webdriver 
'''
Created on 2017年3月15日

@author: libin_m
'''
browser = webdriver.Chrome()
browser.get("http://www.baidu.com")

