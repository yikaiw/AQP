import csv
import copy
import requests
import time
import datetime

STATIONS = ['dongsi_aq', 'tiantan_aq', 'guanyuan_aq', 'wanshouxigong_aq', 'aotizhongxin_aq', 'nongzhanguan_aq', 'wanliu_aq', 'beibuxinqu_aq', 'zhiwuyuan_aq', 'fengtaihuayuan_aq', 'yungang_aq', 'gucheng_aq', 'fangshan_aq', 'daxing_aq', 'yizhuang_aq', 'tongzhou_aq', 'shunyi_aq', 'pingchang_aq', 'mentougou_aq', 'pinggu_aq',
           'huairou_aq', 'miyun_aq', 'yanqin_aq', 'dingling_aq', 'badaling_aq', 'miyunshuiku_aq', 'donggaocun_aq', 'yongledian_aq', 'yufa_aq', 'liulihe_aq', 'qianmen_aq', 'yongdingmennei_aq', 'xizhimenbei_aq', 'nansanhuan_aq', 'dongsihuan_aq', 'CD1', 'BL0', 'GR4', 'MY7', 'HV1', 'GN3', 'GR9', 'LW2', 'GN0', 'KF1', 'CD9', 'ST5', 'TH4']
POLLUTIONS = ['PM25_Concentration', 'PM10_Concentration', 'O3_Concentration']


class UrlReader(object):
    def __init__(self, duration='2018-04-1-0/2018-04-23-23'):
        self.duration = duration
        self.bj_url = 'https://biendata.com/competition/airquality/bj/' + duration + '/2k0d1d8'
        self.ld_url = 'https://biendata.com/competition/airquality/ld/' + duration + '/2k0d1d8'
        self.output_file_name = ['average-bj.csv', 'average-ld.csv']

    def read_with_time(self, city="bj"):
        start_date = time.strptime(self.duration.split('/')[0], "%Y-%m-%d")
        start_date = datetime.datetime(*start_date[:2])
        station, day, hour, pm25, pm10, o3 = [], [], [], [], [], []
        url = self.bj_url if city == "bj" else self.ld_url

        a = requests.get(url)
        reader = csv.DictReader(a.text.splitlines(), delimiter=',')
        for row in reader:
            if row['station_id'] not in STATIONS:
                continue
            if (not row['PM25_Concentration']) or (not row['PM10_Concentration']) or (not row['O3_Concentration']):
                continue
            row_time = time.strptime(row['time'], "%Y-%m-%d %H:%M:%S")
            row_date = datetime.datetime(*row_time[:2])
            delta = row_date - start_date
            day.append(delta.days)
            hour.append(row_time.tm_hour)
            station.append(STATIONS.index(row['station_id']))
            pm25.append(float(row['PM25_Concentration']))
            pm10.append(float(row['PM10_Concentration']))
            o3.append(float(row['O3_Concentration']))

    def save_average_data(self):
        # average wrt days, get various data via hours
        for i, url in enumerate([self.bj_url, self.ld_url]):
            value_dict, count_dict = self.crawler_average(url=url)

            for key, value in value_dict.items():
                for time_key, time_value in value.items():
                    for element_key in time_value.keys():
                        if count_dict[key][time_key][element_key]:
                            value_dict[key][time_key][element_key] /= count_dict[key][time_key][element_key]

            write_file = open(self.output_file_name[i], 'w')
            print(self.output_file_name[i])
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

    def crawler_average(self, url=None):
        value_dict = {name:
                          {str(num).zfill(2): {'PM10_Concentration': 0, 'PM25_Concentration': 0, 'O3_Concentration': 0}
                           for num in range(0, 24)}
                      for name in STATIONS}
        count_dict = copy.deepcopy(value_dict)

        a = requests.get(url)
        reader = csv.DictReader(a.text.splitlines(), delimiter=',')

        for row in reader:
            date = row['time'].split(' ')
            hour = date[1].split(':')[0]
            if row['station_id'] not in STATIONS:
                continue
            for name in POLLUTIONS:
                if row[name]:
                    value_dict[row['station_id']][str(hour)][name] += float(row[name])
                    count_dict[row['station_id']][str(hour)][name] += 1

        return value_dict, count_dict


class CsvReader(object):
    def __init__(self, file_name='sub-bj'):
        self.file_name = file_name

    def read(self):
        value_dict, count_dict = self.crawler_average(csv_file=self.file_name)

        for key, value in value_dict.items():
            for time_key, time_value in value.items():
                for pollution in time_value.keys():
                    if count_dict[key][time_key][pollution]:
                        value_dict[key][time_key][pollution] /= count_dict[key][time_key][pollution]

        return value_dict

    def crawler_average(self, csv_file=None):
        value_dict = {name:
                          {str(num).zfill(2): {'PM10_Concentration': 0, 'PM25_Concentration': 0, 'O3_Concentration': 0}
                           for num in range(0, 24)}
                      for name in STATIONS}
        count_dict = copy.deepcopy(value_dict)

        reader = csv.DictReader(csv_file, delimiter=',')

        for row in reader:
            date = row['time'].split(' ')
            hour = date[1].split(':')[0]
            if row['station_id'] not in STATIONS:
                continue
            for name in POLLUTIONS:
                if row[name]:
                    value_dict[row['station_id']][str(hour)][name] += float(row[name])
                    count_dict[row['station_id']][str(hour)][name] += 1

        return value_dict, count_dict


if __name__ == '__main__':
    url_reader = UrlReader()
    url_reader.save_average_data()