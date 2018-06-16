# Using pandas to process data
import numpy as np
import pandas as pd
import datetime
import sys  



from utils import download_util
from utils.generate_util import generate_training_set

ldstation_list = [
'BL0', 
'CD9',
'CD1',
'GN0',
'GR4',
'GN3',
'GR9',
'HV1',
'KF1',
'LW2',
'ST5',
'TH4','MY7']

station_list = ['dongsi_aq','tiantan_aq','guanyuan_aq','wanshouxigong_aq','aotizhongxin_aq',
            'nongzhanguan_aq','wanliu_aq','beibuxinqu_aq','zhiwuyuan_aq','fengtaihuayuan_aq',
            'yungang_aq','gucheng_aq','fangshan_aq','daxing_aq','yizhuang_aq','tongzhou_aq',
            'shunyi_aq','pingchang_aq','mentougou_aq','pinggu_aq','huairou_aq','miyun_aq',
            'yanqin_aq','dingling_aq','badaling_aq','miyunshuiku_aq','donggaocun_aq',
            'yongledian_aq','yufa_aq','liulihe_aq','qianmen_aq','yongdingmennei_aq',
            'xizhimenbei_aq','nansanhuan_aq','dongsihuan_aq']            
ldX_aq_list = ["PM2.5","PM10","NO2"]

X_aq_list = ["PM2.5","PM10","O3","CO","SO2","NO2"]  

ldy_aq_list = ["PM2.5","PM10"]

y_aq_list = ["PM2.5","PM10","O3"]

X_meo_list = ["temperature","pressure","humidity","direction","speed/kph"]

if __name__ == '__main__':
    
    #generate training set
    X_batch, y_batch = generate_training_set(station_list, y_aq_list, y_aq_list, X_meo_list)
    

