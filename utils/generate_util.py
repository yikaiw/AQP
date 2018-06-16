import csv
import copy
import requests
import time as tm
import datetime
import numpy as np
import pandas as pd


def generate_training_set(station_list, X_aq_list, y_aq_list, X_meo_list=None,city = 'bj'):
    '''
    
    Args:
        station_list : a list of used stations.
        X_aq_list : a list of used aq features as input.
        y_aq_list : a list of used aq features as output. 
        X_meo_list : a list of used meo features.
        use_day : bool, True to just use 0-24 h days.
        pre_days : use pre_days history days to predict.
        batch_size
        
    station_list = ['dongsi_aq','tiantan_aq','guanyuan_aq','wanshouxigong_aq','aotizhongxin_aq',
                'nongzhanguan_aq','wanliu_aq','beibuxinqu_aq','zhiwuyuan_aq','fengtaihuayuan_aq',
                'yungang_aq','gucheng_aq','fangshan_aq','daxing_aq','yizhuang_aq','tongzhou_aq',
                'shunyi_aq','pingchang_aq','mentougou_aq','pinggu_aq','huairou_aq','miyun_aq',
                'yanqin_aq','dingling_aq','badaling_aq','miyunshuiku_aq','donggaocun_aq',
                'yongledian_aq','yufa_aq','liulihe_aq','qianmen_aq','yongdingmennei_aq',
                'xizhimenbei_aq','nansanhuan_aq','dongsihuan_aq']            
    X_aq_list = ["PM2.5","PM10","O3","CO","SO2","NO2"]  
    y_aq_list = ["PM2.5","PM10","O3"]
    X_meo_list = ["temperature","pressure","humidity","direction","speed/kph"]
    '''
    
    aq_train = pd.read_csv("preprocess/"+city+"_aq_data.csv")
    meo_train = pd.read_csv("preprocess/"+city+"_meo_data.csv")
    

    #dev_df = pd.concat([aq_train, meo_train], axis=1)
    dev_df = pd.merge(aq_train, meo_train, on='time')


    # step 1 : keep all features about the stations
    station_filters = []
    for station in station_list : 
        station_filter = [index for index in dev_df.columns if station in index]
        station_filters += station_filter
    
    # step 2 : filter of X features
    X_feature_filters = []
    if X_meo_list :
        X_features = X_aq_list + X_meo_list
    else :
        X_features = X_aq_list
        
    for i in station_filters : 
        if i.split("_")[-1] in X_features :
            X_feature_filters += [i]
    X_feature_filters.append('time')
    X_feature_filters.sort()  # 排序，保证�??练集和验证集�?的特征的顺序一�?
    
    # step 3: add time feature
    X_df = dev_df[X_feature_filters]
    hour_list = []
    for item in X_df['time']:
        hour = tm.strptime(item, '%Y-%m-%d %H:%M:%S')
        hour_list.append(hour.tm_hour)
    X_df['hour'] = pd.Series(hour_list)
    print("总样�?�?�?:",X_df.shape)
    
    '''
    # step 4 : filter of y features
    y_feature_filters = []
    y_features = y_aq_list
    

    for i in station_filters : 
        if i.split("_")[-1] in y_features :
            y_feature_filters += [i]
    
    y_feature_filters.append('time')
    y_feature_filters.sort()  # 排序，保证�??练集和验证集�?的特征的顺序一�?
    y_df = dev_df[y_feature_filters]
    hour_list = []
    for item in y_df['time']:
        hour = tm.strptime(item, '%Y-%m-%d %H:%M:%S')
        hour_list.append(hour.tm_hour)
    y_df['hour'] = pd.Series(hour_list)
    '''
    
    # step 5: remove nan value
    x_list = []
    #y_list = []
    for i in range(0,X_df.shape[0]):
        flag = 1
        for item in list(X_df.iloc[i,]) :
            if type(item) == type('str'):
                continue
            if np.isnan(item):
                flag = 0
        '''
        for item in list(y_df.iloc[i+2,]):
            if type(item) == type('str'):
                continue
            if np.isnan(item):
                flag = 0
        '''
        if flag == 1:
            x_list.append(i)
            #y_list.append(i+2)
    #print(x_list)
    x = X_df.loc[x_list,]
    print("去除NAN后样�?�?数：",x.shape)

    #y = y_df.loc[y_list,]
     
    #x_list.to_csv("output/xlist.csv")
    x.to_csv("generateData/"+city+"Xdata.csv",index=False,header=True)
    #y.to_csv("generateData/"+city+"Ydata.csv",index=False)


    

    '''
    # step 4 : generate training batch
    X_df_list = []
    y_df_list = []
    
    max_start_points = X_df.shape[0] - (pre_days + 2) * 24
    if use_day : 
        total_start_points = range(0, max_start_points, 24)
    else :
        total_start_points = range(0, max_start_points, 1)
    
    for i in range(batch_size):       
        flag = True        
        while flag :
            X_start_index = int(np.random.choice(total_start_points, 1, replace = False))
            X_end_index = X_start_index + pre_days * 24 - 1

            y_start_index = X_end_index + 1
            y_end_index = X_end_index + 48
    
            # print(X_start_index, X_end_index, y_start_index, y_end_index)

            X = X_df.loc[X_start_index : X_end_index]
            y = y_df.loc[y_start_index : y_end_index]

            # 判断�?不是�? NAN
            if pd.isnull(X).any().any() or pd.isnull(y).any().any():
                pass
            else :     
                X = np.array(X)
                y = np.array(y)
                X = np.expand_dims(X, axis=0)
                y = np.expand_dims(y, axis=0)
                X_df_list.append(X)
                y_df_list.append(y)
                flag = False

    X_train_batch = np.concatenate(X_df_list, axis=0)
    y_train_batch = np.concatenate(y_df_list, axis=0)
    '''
    return x, y

def generate_test_set(dev_df,station_list, X_aq_list, y_aq_list, X_meo_list=None):
    
    # step 1 : keep all features about the stations
    station_filters = []
    for station in station_list : 
        station_filter = [index for index in dev_df.columns if station in index]
        station_filters += station_filter
    
    # step 2 : filter of X features
    X_feature_filters = []
    if X_meo_list :
        X_features = X_aq_list + X_meo_list
    else :
        X_features = X_aq_list
        
    for i in station_filters : 
        if i.split("_")[-1] in X_features :
            X_feature_filters += [i]
    X_feature_filters.sort()  # 排序，保证�??练集和验证集�?的特征的顺序一�?
    
    # step 3: add time feature
    X_df = dev_df[X_feature_filters]
    
    return X_df
