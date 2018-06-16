import pandas as pd
import numpy as np
import datetime
import time as tm

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
aq_list = ["PM2.5","PM10","O3"]
ldaq_list = ["PM2.5","PM10"]
output = {"test_id":[],"PM2.5":[],"PM10":[],"O3":[]}
id2index = {}
index2id = {}
index = 0


for item in station_list:
    for hour in range(0,48):
        test_id = item+"#"+ str(hour)
        id2index[test_id] = index
        index2id[index] = test_id
        output["test_id"].append(test_id)
        output["PM2.5"].append(0)
        output["PM10"].append(0)
        output["O3"].append(0)
        index += 1
        
for item in ldstation_list:
    for hour in range(0,48):
        test_id = item+"#"+ str(hour)
        id2index[test_id] = index
        index2id[index] = test_id
        output["test_id"].append(test_id)
        output["PM2.5"].append(0)
        output["PM10"].append(0)
        output["O3"].append(0)
        index += 1
        

    
#load data
sheet_name = "Sheet3"

bj = pd.read_excel("bj_aq_data.xlsx",sheet_name=sheet_name)
ld = pd.read_excel("ld_aq_data.xlsx",sheet_name=sheet_name)

print("bj:",bj.shape[0])
print("ld:",ld.shape[0])

#remove nan
bj_list = []
for i in range(0,bj.shape[0]):
        flag = 1
        for item in list(bj.iloc[i,]) :
            try:
                if np.isnan(item):
                    flag = 0
            except:
                pass
        if flag == 1 :
            bj_list.append(i)
            
bj = bj.loc[bj_list,] 

ld_list = []
for i in range(0,ld.shape[0]):
        flag = 1
        for item in list(ld.iloc[i,]) :
            try:
                if np.isnan(item):
                    flag = 0
            except:
                pass
        if flag == 1 :
            ld_list.append(i)
            
ld = ld.loc[ld_list,] 

print("after NAN, bj",bj.shape[0])
print("after NAN ld",ld.shape[0])

hour2times={}
for i in range(0,24):
    hour2times[i] = 0
for item in bj['time']:
    hour = item.hour
    hour2times[hour] += 1
for i in range(0,bj.shape[0]):
    row = bj.iloc[i,]
    time = row['time']
    
    #time = tm.strptime(time, '%Y-%m-%d %H:%M:%S')
    hour = time.hour
    for item in station_list:
        test_id1 = item+"#"+str(hour)
        test_id2 = item+"#"+str(hour+24)
        index1 = id2index[test_id1]
        index2 = id2index[test_id2]
        
        for f in aq_list:
            feature = item + "_" + f
            output[f][index1] += row[feature]/hour2times[hour]
            output[f][index2] += row[feature]/hour2times[hour]

for i in range(0,24):
    hour2times[i] = 0
for item in ld['time']:
    hour = item.hour
    hour2times[hour] += 1
for i in range(0,ld.shape[0]):
    row = ld.iloc[i,]
    time = row['time']
    hour = time.hour
    for item in ldstation_list:
        test_id1 = item+"#"+str(hour)
        test_id2 = item+"#"+str(hour+24)
        index1 = id2index[test_id1]
        index2 = id2index[test_id2]
        
        for f in ldaq_list:
            feature = item + "_" + f
            output[f][index1] += row[feature]/hour2times[hour]
            output[f][index2] += row[feature]/hour2times[hour]
    
output = pd.DataFrame(output)
columns = ['test_id','PM2.5','PM10','O3']
output.to_csv("3avg.csv",columns = columns)
print("ok!")


