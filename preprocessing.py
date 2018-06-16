# Using pandas to process data
import numpy as np
import pandas as pd
import datetime
import sys  

from utils.aq_data_util import load_aq_data2
from utils.weather_data_util import load_grid_meo_data,get_station_locations,find_nearst_meo_station_name

bj_near_stations = {'aotizhongxin_aq': 'beijing_grid_304',
 'badaling_aq': 'beijing_grid_224',
 'beibuxinqu_aq': 'beijing_grid_263',
 'daxing_aq': 'beijing_grid_301',
 'dingling_aq': 'beijing_grid_265',
 'donggaocun_aq': 'beijing_grid_452',
 'dongsi_aq': 'beijing_grid_303',
 'dongsihuan_aq': 'beijing_grid_324',
 'fangshan_aq': 'beijing_grid_238',
 'fengtaihuayuan_aq': 'beijing_grid_282',
 'guanyuan_aq': 'beijing_grid_282',
 'gucheng_aq': 'beijing_grid_261',
 'huairou_aq': 'beijing_grid_349',
 'liulihe_aq': 'beijing_grid_216',
 'mentougou_aq': 'beijing_grid_240',
 'miyun_aq': 'beijing_grid_392',
 'miyunshuiku_aq': 'beijing_grid_414',
 'nansanhuan_aq': 'beijing_grid_303',
 'nongzhanguan_aq': 'beijing_grid_324',
 'pingchang_aq': 'beijing_grid_264',
 'pinggu_aq': 'beijing_grid_452',
 'qianmen_aq': 'beijing_grid_303',
 'shunyi_aq': 'beijing_grid_368',
 'tiantan_aq': 'beijing_grid_303',
 'tongzhou_aq': 'beijing_grid_366',
 'wanliu_aq': 'beijing_grid_283',
 'wanshouxigong_aq': 'beijing_grid_303',
 'xizhimenbei_aq': 'beijing_grid_283',
 'yanqin_aq': 'beijing_grid_225',
 'yizhuang_aq': 'beijing_grid_323',
 'yongdingmennei_aq': 'beijing_grid_303',
 'yongledian_aq': 'beijing_grid_385',
 'yufa_aq': 'beijing_grid_278',
 'yungang_aq': 'beijing_grid_239',
 'zhiwuyuan_aq': 'beijing_grid_262'}

ld_near_stations = {'BL0': 'london_grid_409', 
'CD9': 'london_grid_409',
'CD1': 'london_grid_388',
'GN0': 'london_grid_451',
'GR4': 'london_grid_451',
'GN3': 'london_grid_451',
'GR9': 'london_grid_430',
'HV1': 'london_grid_472',
'KF1': 'london_grid_388',
'LW2': 'london_grid_430',
'ST5': 'london_grid_408',
'TH4': 'london_grid_430','MY7': 'london_grid_388'}

if __name__ == '__main__':
    #preprocessing aq data
    bj_aq_data = load_aq_data2('bj')
    ld_aq_data = load_aq_data2('ld')
    
    #preprocessing meo data
    bj_meo_stations = load_grid_meo_data('bj',bj_near_stations)
    ld_meo_stations = load_grid_meo_data('ld',ld_near_stations)

    

