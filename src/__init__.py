#encoding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import re
import os
if __name__=='__main__':
    phanzi=re.compile(u'[\u4e00-\u9fa5]')
    b = u'中国人'
    print phanzi.findall(b)
    with open(r'c:\Users\libin_m\Desktop\wmy_tlbb3d_ls_2016-01-01.txt','r') as f, open(r'.\new_data.txt','w') as w:
        for line in f.xreadlines():
            li = phanzi.findall(line.decode('utf-8'))
            if li:
                continue
            w.write('%s' % (line))
            
                
                