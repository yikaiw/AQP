import csv
import copy
import requests
import time
import datetime

STATIONS = {'bj': ['dongsi_aq', 'tiantan_aq', 'guanyuan_aq', 'wanshouxigong_aq', 'aotizhongxin_aq', 'nongzhanguan_aq', 'wanliu_aq', 'beibuxinqu_aq', 'zhiwuyuan_aq', 'fengtaihuayuan_aq', 'yungang_aq', 'gucheng_aq', 'fangshan_aq', 'daxing_aq', 'yizhuang_aq', 'tongzhou_aq', 'shunyi_aq',
                  'pingchang_aq', 'mentougou_aq', 'pinggu_aq', 'huairou_aq', 'miyun_aq', 'yanqin_aq', 'dingling_aq', 'badaling_aq', 'miyunshuiku_aq', 'donggaocun_aq', 'yongledian_aq', 'yufa_aq', 'liulihe_aq', 'qianmen_aq', 'yongdingmennei_aq', 'xizhimenbei_aq', 'nansanhuan_aq', 'dongsihuan_aq'],
            'ld': ['CD1', 'BL0', 'GR4', 'MY7', 'HV1', 'GN3', 'GR9', 'LW2', 'GN0', 'KF1', 'CD9', 'ST5', 'TH4']}
POLLUTIONS = ['PM25_Concentration', 'PM10_Concentration', 'O3_Concentration']


def crawler_average(url=None, csv_file=None):
    value_dict = {name:
                      {str(num).zfill(2): {'PM10_Concentration': 0, 'PM25_Concentration': 0, 'O3_Concentration': 0}
                       for num in range(0, 24)}
                  for name in STATIONS}
    count_dict = copy.deepcopy(value_dict)

    if url is not None:
        a = requests.get(url)
        reader = csv.DictReader(a.text.splitlines(), delimiter=',')
    else:
        reader = csv.DictReader(csv_file, delimiter=',')

    for line in reader:
        date = line['time'].split(' ')
        hour = date[1].split(':')[0]
        if line['station_id'] not in STATIONS:
            continue
        for name in POLLUTIONS:
            if line[name]:
                value_dict[line['station_id']][str(hour)][name] += float(line[name])
                count_dict[line['station_id']][str(hour)][name] += 1

    return value_dict, count_dict


class UrlReader(object):
    def __init__(self, duration='2018-04-1-0/2018-04-23-23'):
        self.duration = duration
        self.bj_url = 'https://biendata.com/competition/airquality/bj/' + duration + '/2k0d1d8'
        self.ld_url = 'https://biendata.com/competition/airquality/ld/' + duration + '/2k0d1d8'
        self.output_file_name = ['average-bj.csv', 'average-ld.csv']

    def read_with_time(self, city='bj'):
        start_date = time.strptime(self.duration.split('/')[0], '%Y-%m-%d-%H')
        start_date = datetime.datetime(*start_date[:3])
        end_date = time.strptime(self.duration.split('/')[1], '%Y-%m-%d-%H')
        end_date = datetime.datetime(*end_date[:3])
        delta = end_date - start_date
        url_data = {'station': [], 'day': [], 'hour': [], 'pm25': [], 'pm10': [], 'o3': [], 'day_delta': delta.days}
        url = self.bj_url if city == 'bj' else self.ld_url
        a = requests.get(url)
        reader = csv.DictReader(a.text.splitlines(), delimiter=',')

        for line in reader:
            if line['station_id'] not in STATIONS[city]:
                continue
            if (not line['PM25_Concentration']) or (not line['PM10_Concentration']) or (not line['O3_Concentration']):
                continue
            line_time = time.strptime(line['time'], '%Y-%m-%d %H:%M:%S')
            line_date = datetime.datetime(*line_time[:3])
            delta = line_date - start_date
            url_data['day'].append(delta.days)
            url_data['hour'].append(line_time.tm_hour)
            url_data['station'].append(STATIONS[city].index(line['station_id']))
            url_data['pm25'].append(float(line['PM25_Concentration']))
            url_data['pm10'].append(float(line['PM10_Concentration']))
            url_data['o3'].append(float(line['O3_Concentration']))

        return url_data

    def save_average_data(self):
        # average wrt days, get various data via hours
        for i, url in enumerate([self.bj_url, self.ld_url]):
            value_dict, count_dict = crawler_average(url=url)

            for key, value in value_dict.items():
                for time_key, time_value in value.items():
                    for element_key in time_value.keys():
                        if count_dict[key][time_key][element_key]:
                            value_dict[key][time_key][element_key] /= count_dict[key][time_key][element_key]

            write_file = open(self.output_file_name[i], 'w')
            print('test_id,PM2.5,PM10,O3', file=write_file)

            for key, value in value_dict.items():
                for time_key, time_value in value.items():
                    print(key, file=write_file, end='')
                    print('#', int(time_key), file=write_file, end='', sep='')
                    for pollution in POLLUTIONS:
                        print(',', int(round(value_dict[key][time_key][pollution])), file=write_file, end='', sep='')
                    print('', file=write_file, end='\n')

            write_file.close()

        return self.output_file_name


class CsvReader(object):
    def __init__(self, file_name='sub-bj'):
        self.file_name = file_name

    def read_average_data(self):
        value_dict, count_dict = crawler_average(csv_file=self.file_name)

        for key, value in value_dict.items():
            for time_key, time_value in value.items():
                for pollution in time_value.keys():
                    if count_dict[key][time_key][pollution]:
                        value_dict[key][time_key][pollution] /= count_dict[key][time_key][pollution]

        return value_dict


class CsvSaver(object):
    def __init__(self, x_data, y_data, city='bj', filename='untitled.csv'):
        self.x_data = x_data  # (None, 3) station_id, day, hour
        self.y_data = y_data  # (None, 3) pm25, pm10, o3
        self.city = city
        self.filename = filename

    def save(self):
        write_file = open(self.filename, 'w')
        print('test_id,PM2.5,PM10,O3', file=write_file)
        for i in range(self.x_data.shape[0]):
            station_id = int(self.x_data[i][0])
            print(STATIONS[self.city][station_id], file=write_file, end='')
            hour = int(self.x_data[i][2])
            print('#', hour, file=write_file, end='', sep='')
            for pollution in self.y_data[i]:
                print(',', int(round(pollution)), file=write_file, end='', sep='')
            print('', file=write_file, end='\n')
        write_file.close()
