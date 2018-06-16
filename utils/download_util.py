import csv
import copy
import requests
import time
import datetime
import numpy as np
import pandas as pd


STATIONS = {'bj': ['dongsi_aq', 'tiantan_aq', 'guanyuan_aq', 'wanshouxigong_aq', 'aotizhongxin_aq', 'nongzhanguan_aq', 'wanliu_aq', 'beibuxinqu_aq', 'zhiwuyuan_aq', 'fengtaihuayuan_aq', 'yungang_aq', 'gucheng_aq', 'fangshan_aq', 'daxing_aq', 'yizhuang_aq', 'tongzhou_aq', 'shunyi_aq',
                  'pingchang_aq', 'mentougou_aq', 'pinggu_aq', 'huairou_aq', 'miyun_aq', 'yanqin_aq', 'dingling_aq', 'badaling_aq', 'miyunshuiku_aq', 'donggaocun_aq', 'yongledian_aq', 'yufa_aq', 'liulihe_aq', 'qianmen_aq', 'yongdingmennei_aq', 'xizhimenbei_aq', 'nansanhuan_aq', 'dongsihuan_aq'],
            'ld': ['CD1', 'BL0', 'GR4', 'MY7', 'HV1', 'GN3', 'GR9', 'LW2', 'GN0', 'KF1', 'CD9', 'ST5', 'TH4']}
POLLUTIONS = ['PM25_Concentration', 'PM10_Concentration', 'O3_Concentration']

class UrlReader(object):
    def __init__(self, duration='2018-04-1-0/2018-04-23-23',time = '2018-05-02-14'):
        self.duration = duration
        self.time = time
        self.bj_url = 'https://biendata.com/competition/airquality/bj/' + duration + '/2k0d1d8'
        self.ld_url = 'https://biendata.com/competition/airquality/ld/' + duration + '/2k0d1d8'
        
        self.bj_meo_url = 'https://biendata.com/competition/meteorology/bj_grid/'+duration+'/2k0d1d8'
        self.ld_meo_url = 'https://biendata.com/competition/meteorology/ld_grid/'+duration+'/2k0d1d8'

        self.bj_fore = "http://kdd.caiyunapp.com/competition/forecast/bj/"+time+"/2k0d1d8"
        self.ld_fore = "http://kdd.caiyunapp.com/competition/forecast/ld/"+time+"/2k0d1d8"

    def read_aq_with_time(self, city='bj'):
        start_date = time.strptime(self.duration.split('/')[0], '%Y-%m-%d-%H')
        start_date = datetime.datetime(*start_date[:3])
        end_date = time.strptime(self.duration.split('/')[1], '%Y-%m-%d-%H')
        end_date = datetime.datetime(*end_date[:3])
        delta = end_date - start_date
        url_data = {'stationId': [], 'utc_time': [], 'PM2.5':[],'PM10': [], 'NO2': [], 'CO': [],'O3': [], 'SO2': []}
        url = self.bj_url if city == 'bj' else self.ld_url
        
        print(url)
        a = requests.get(url)
        print("request ok")

        reader = csv.DictReader(a.text.splitlines(), delimiter=',')

        for line in reader:
            if line['station_id'] not in STATIONS[city]:
                continue
            url_data['stationId'].append(line['station_id'])
            url_data['utc_time'].append(line['time'])
            url_data['PM2.5'].append(line['PM25_Concentration'])
            url_data['PM10'].append(line['PM10_Concentration'])
            url_data['O3'].append(line['O3_Concentration'])
            url_data['SO2'].append(line['SO2_Concentration'])
            url_data['CO'].append(line['CO_Concentration'])
            url_data['NO2'].append(line['NO2_Concentration'])


        df_url_data = pd.DataFrame(url_data)
        #df_url_data.set_index("utc_time", inplace=True)
        df_url_data.set_index("stationId", inplace=True)
        columns = ['utc_time', 'PM2.5','PM10', 'NO2', 'CO','O3', 'SO2']
        df_url_data.to_csv("download/"+city+"_"+self.duration.replace('/','')+"_aq_data.csv",columns=columns)

        return df_url_data

    def read_meo_with_time(self, city='bj'):
        start_date = time.strptime(self.duration.split('/')[0], '%Y-%m-%d-%H')
        start_date = datetime.datetime(*start_date[:3])
        end_date = time.strptime(self.duration.split('/')[1], '%Y-%m-%d-%H')
        end_date = datetime.datetime(*end_date[:3])
        delta = end_date - start_date
        url_data = {'stationName':[],
        'utc_time':[],
        'weather':[],
        'temperature':[],
        'pressure':[],
        'humidity':[],
        'wind_direction':[],
        'wind_speed/kph':[]}
        
        url = self.bj_meo_url if city == 'bj' else self.ld_meo_url
        print(url)
        a = requests.get(url)
        print("request ok")
        reader = csv.DictReader(a.text.splitlines(), delimiter=',')

        for line in reader:
            
            url_data['stationName'].append(line['station_id'])
            url_data['utc_time'].append(line['time'])
            url_data['weather'].append(line['weather'])
            url_data['temperature'].append(line['temperature'])
            url_data['pressure'].append(line['pressure'])
            url_data['humidity'].append(line['humidity'])
            
            url_data['wind_direction'].append(line['wind_direction'])
            url_data['wind_speed/kph'].append(line['wind_speed'])

        df_url_data = pd.DataFrame(url_data)
        df_url_data.set_index("stationName", inplace=True)
                         
        columns = [ 'utc_time','weather', 'temperature','pressure', 'humidity', 'wind_direction','wind_speed/kph']
        df_url_data.to_csv("download/"+city+"_"+self.duration.replace('/','')+"_meo_data.csv",columns=columns)
        return df_url_data

    def read_fore(self, city='bj'):
    
        url_data = {'stationName':[],
        'utc_time':[],
        'temperature':[],
        'pressure':[],
        'humidity':[],
        'wind_direction':[],
        'wind_speed/kph':[],
        'weather':[]}
        
        url = self.bj_fore if city == 'bj' else self.ld_fore
        print(url)
        a = requests.get(url)
        print("request ok")
        reader = csv.DictReader(a.text.splitlines(), delimiter=',')

        for line in reader:
            
            url_data['stationName'].append(line['station_id'])
            url_data['utc_time'].append(line['forecast_time'])
            url_data['weather'].append(line['weather'])
            url_data['temperature'].append(line['temperature'])
            url_data['pressure'].append(line['pressure'])
            url_data['humidity'].append(line['humidity'])
            
            url_data['wind_direction'].append(line['wind_direction'])
            url_data['wind_speed/kph'].append(line['wind_speed'])
        
        df_url_data = pd.DataFrame(url_data)
        df_url_data.set_index("stationName", inplace=True)
                         
        columns = [ 'utc_time','weather', 'temperature','pressure', 'humidity', 'wind_direction','wind_speed/kph']
        df_url_data.to_csv("download/"+city+"_"+self.time.replace('/','')+"_fore_data.csv",columns=columns)
        return df_url_data
