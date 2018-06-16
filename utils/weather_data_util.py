import numpy as np
import pandas as pd
import datetime
import time as tm
from matplotlib import pyplot as plt

def load_grid_meo_data(city,useful_stations,filename=""):
    '''
    useful_stations : dict of {aq_station : meo_station}
    '''
    bj_grid_meo_dataset_1 = ''
    if filename != "":
        bj_grid_meo_dataset_1 = pd.read_csv(filename)
    elif city =='bj':
        bj_grid_meo_dataset_1 = pd.read_csv("download/Beijing_historical_meo_grid-Copy1.csv")
        #bj_grid_meo_dataset_1 = pd.read_csv("download/bj_2018-04-01-002018-05-15-23_meo_data.csv")
    else:
        bj_grid_meo_dataset_1 = pd.read_csv("download/London_historical_meo_grid-Copy1.csv")
        #bj_grid_meo_dataset_1 = pd.read_csv("download/ld_2018-04-01-002018-05-15-23_meo_data.csv")
    # 去掉位置信息
    if "longitude" in bj_grid_meo_dataset_1.columns:
        bj_grid_meo_dataset_1.drop("longitude", axis=1, inplace=True)
        bj_grid_meo_dataset_1.drop("latitude", axis=1, inplace=True)

    # API 下载数据
    #bj_grid_meo_dataset_2 = pd.read_csv("orginal_data/Beijing/grid_meo/new.csv")

    #bj_grid_meo_dataset = pd.concat([bj_grid_meo_dataset_1, bj_grid_meo_dataset_2], ignore_index=True)

    bj_grid_meo_dataset =bj_grid_meo_dataset_1

    # turn date from string type to datetime type
    bj_grid_meo_dataset["time"] = pd.to_datetime(bj_grid_meo_dataset['utc_time'])
    bj_grid_meo_dataset.set_index("time", inplace=True)
    bj_grid_meo_dataset.drop("utc_time", axis=1, inplace=True)

    # names of all stations
    stations = set(bj_grid_meo_dataset['stationName'])

    # a dict of station aq, Beijing
    bj_meo_stations = {}

    for aq_station, meo_station in useful_stations.items() :

    	if meo_station in stations :
	        bj_meo_station = bj_grid_meo_dataset[bj_grid_meo_dataset["stationName"]==meo_station].copy()
	        bj_meo_station.drop("stationName", axis=1, inplace=True)

	        # rename
	        original_names = bj_meo_station.columns.values.tolist()
	        names_dict = {original_name : aq_station+"_"+original_name for original_name in original_names}
	        bj_meo_station_renamed = bj_meo_station.rename(index=str, columns=names_dict)
	        

	        bj_meo_stations[aq_station] = bj_meo_station_renamed

    #df=[]
    ##去除重�?�数�?
    for station in bj_meo_stations.keys():
        df = bj_meo_stations[station].copy()
    length = df.shape[0]
    order = range(length)
    df['order'] = pd.Series(order, index=df.index)
    df["time"] = df.index
    df.set_index("order", inplace=True)
    
    length_1 = df.shape[0]
    print("重�?�值去除之前，共有数据数量", df.shape[0])
    
    used_times = []
    for index in df.index :
        time = df.loc[index]["time"]
        if time not in used_times :
            used_times.append(time)
        else : 
            df.drop([index], inplace=True)

    length_2 = df.shape[0]
    delta = length_1 - length_2
    print("重�?�值去除之后，共有数据数量", df.shape[0])
    #print("%s 重�?�数�? : %d" %(station, delta))
    
    df.set_index("time", inplace=True)
    bj_meo_stations[station] = df

    
    for station in bj_meo_stations.keys() :
        df = bj_meo_stations[station].copy()
        min_time = df.index.min()
        max_time = df.index.max()
        min_time = datetime.datetime.strptime(min_time, '%Y-%m-%d %H:%M:%S')
        max_time = datetime.datetime.strptime(max_time, '%Y-%m-%d %H:%M:%S')
        delta_all = max_time - min_time
        all_length = delta_all.total_seconds()/3600 + 1
        real_length = df.shape[0]
        print(min_time)
        print(max_time)
        #print("在空气质量数�?时间段内，总共应�?�有 %d �?小时节点�?" %(all_length))
        # print("实际的时间节点数�? ", real_length)
        print("%s 缺失时间节点数量�? %d" %(station, all_length-real_length))
    
    #�?充NAN
    delta = datetime.timedelta(hours=1)
    for station in bj_meo_stations.keys() :
        df = bj_meo_stations[station].copy()
        nan_series = pd.Series({key:np.nan for key in df.columns})
        min_time = df.index.min()
        max_time = df.index.max()
        min_time = datetime.datetime.strptime(min_time, '%Y-%m-%d %H:%M:%S')
        max_time = datetime.datetime.strptime(max_time, '%Y-%m-%d %H:%M:%S')

        time = min_time
        while time <=  max_time :
            time_str = datetime.date.strftime(time, '%Y-%m-%d %H:%M:%S')
            if time_str not in df.index : 
                df.loc[time_str] = nan_series
            time += delta
        bj_meo_stations[station] = df
    
    
    #风向缺失处理
    for station in bj_meo_stations.keys():
        df = bj_meo_stations[station].copy()
        df.replace(999017,0, inplace=True)
        bj_meo_stations[station] = df

    #合并
    bj_meo_stations_merged = pd.concat(list(bj_meo_stations.values()), axis=1)
    bj_meo_stations_merged.sort_index(inplace=True)
    bj_meo_stations_merged.index.name = 'time'
    bj_meo_stations_merged.to_csv("preprocess/"+city+"_meo_dataAll.csv")
    #return bj_grid_meo_dataset, stations, bj_meo_stations_merged
    return  bj_meo_stations_merged

def find_nearst_meo_station_name(aq_location, meo_locations):
    '''
    From meo stations ans grid meos stations, find the nearest meo station of aq station.
    Args :
        aq_location : an aq station information of (station_name, (longitude, latitude))
        meo_locations : meo information, list of ((station_name, (longitude, latitude)))
    '''
    nearest_station_name = ""
    nearest_distance = 1e10
    
    aq_station_longitude = aq_location[1][0]
    aq_station_latitude = aq_location[1][1]
    
    for station_name, (longitude, latitude) in meo_locations:
        dis = np.sqrt((longitude-aq_station_longitude)**2 + (latitude-aq_station_latitude)**2)
        if dis < nearest_distance:
            nearest_distance = dis
            nearest_station_name = station_name
    
    return nearest_station_name


def get_station_locations(stations_df):
    '''
    Get all the locations of stations in stations_df.
    Agrs : 
        stations_df : a dataframe of all station data.
    Return : 
        A list of (station_name, (longitude, latitude))
    '''
    
    locations = []
    station_names = []
    station_column_name =""
    if 'station_id' in stations_df.columns:
        station_column_name = 'station_id'
    elif 'stationName' in stations_df.columns:
        station_column_name = 'stationName'
    elif 'stationId' in stations_df.columns:
        station_column_name = 'stationId'
    else :
        print("Can not find station name!")
    
    for j in stations_df.index:
        station_name = stations_df[station_column_name][j]
        if station_name not in station_names:
            station_names.append(station_name)
            longitude = stations_df['longitude'][j]
            latitude = stations_df['latitude'][j]
            location = (longitude, latitude)
            # station_name = stations_df[station_column_name][j]
            locations.append((station_name, location))
    
    return locations






