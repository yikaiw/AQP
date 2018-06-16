# Using pandas to process data
import numpy as np
import pandas as pd
import datetime
import sys  

from utils import download_util


if __name__ == '__main__':
  
    #download aq meo data#
    #duration = '2018-05-31-0/2018-05-03-12'#ld aq
    #duration = '2018-03-31-0/2018-05-03-13'#bj aq bj meo  ld meo

    #duration = '2018-05-08-10/2018-05-08-10'
    #time = '2018-05-08-10'

    duration = '2018-04-01-00/2018-05-15-23'
    time = '2018-05-13-22'
    #下载历史数据
    city = 'bj'
    url_reader = download_util.UrlReader(duration = duration)
    url_data = url_reader.read_aq_with_time(city=city)
    url_data = url_reader.read_meo_with_time(city=city)
    city = 'ld'
    url_data = url_reader.read_aq_with_time(city=city)
    url_data = url_reader.read_meo_with_time(city=city)

    #下载天气预报#
    city = 'bj'
    url_reader = download_util.UrlReader(time = time)
    url_data = url_reader.read_fore(city=city)
    city = 'ld'
    url_reader = download_util.UrlReader(time = time)
    url_data = url_reader.read_fore(city=city)
    

