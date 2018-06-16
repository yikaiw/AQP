import numpy as np
import pandas as pd
import datetime
from matplotlib import pyplot as plt

def load_aq_data(city,filename=""):
	bj_aq_dataset = [];
	# load csvs 
	if filename!='':
		bj_aq_dataset = pd.read_csv(filename);
	elif city == 'bj':
		#bj_aq_dataset = pd.read_csv("/Users/haoyi/Documents/kdd2018/AQP-master/dataG/test/bj_2018-05-09-222018-05-09-22_aq_data.csv")
		bj_aq_dataset = pd.read_csv("download/beijing_17_18_aq.csv")
		'''
		bj_aq_dataset_1 = pd.read_csv("orginal_data/bj/aq/beijing_17_18_aq.csv")
		bj_aq_dataset_2 = pd.read_csv("orginal_data/bj/aq/beijing_201802_201803_aq.csv")

		#bj_aq_dataset_3 = pd.read_csv("orginal_data/Beijing/aq/new.csv")
		#bj_aq_dataset = pd.concat([bj_aq_dataset_1, bj_aq_dataset_2, bj_aq_dataset_3], ignore_index=True)
		bj_aq_dataset = pd.concat([bj_aq_dataset_1,bj_aq_dataset_2], ignore_index=True)
		'''
	elif city == 'ld':
		#bj_aq_dataset  = pd.read_csv("orginal_data/ld/aq/London_historical_aqi_forecast_stations_20180331.csv");
		#bj_aq_dataset = pd.read_csv("/Users/haoyi/Documents/kdd2018/AQP-master/dataG/test/ld_2018-05-09-222018-05-09-22_aq_data.csv")
		bj_aq_dataset = pd.read_csv("download/London_historical_aqi_forecast.csv")
	else:
		return
	
	# turn date from string type to datetime type
	bj_aq_dataset["time"] = pd.to_datetime(bj_aq_dataset['utc_time'])
	bj_aq_dataset.set_index("time", inplace=True)
	bj_aq_dataset.drop("utc_time", axis=1, inplace=True)
	# names of all stations
	stations = set(bj_aq_dataset['stationId'])

	# a dict of station aq, Beijing
	bj_aq_stations = {}

	bj_aq_stations_merged =[]
	for station in stations:
		#dataFrame of station
		bj_aq_station = bj_aq_dataset[bj_aq_dataset["stationId"]==station].copy()
		bj_aq_station.drop("stationId", axis=1, inplace=True)
		# rename
		original_names = bj_aq_station.columns.values.tolist()
		names_dict = {original_name : station+"_"+original_name for original_name in original_names}
		bj_aq_station_renamed = bj_aq_station.rename(index=str, columns=names_dict)
              
		bj_aq_stations[station] = bj_aq_station_renamed
		

	# merge data of different stations into one df
	bj_aq_stations_merged = pd.concat(list(bj_aq_stations.values()), axis=1)
	# add a column of (0,1,2,3,...) to count
	length = bj_aq_stations_merged.shape[0]
	order = range(length)
	bj_aq_stations_merged['order'] = pd.Series(order, index=bj_aq_stations_merged.index)

	print("最早的日期：",bj_aq_stations_merged.index.min())
	print("最晚的日期：", bj_aq_stations_merged.index.max())
	#重复值为相同的数据
	df_merged = bj_aq_stations_merged
	df_merged["time"] = df_merged.index
	df_merged.set_index("order", inplace=True)
	print("重复值去除之前，共有数据数量", df_merged.shape[0])
	used_times = set()
	for index in df_merged.index :
		time = df_merged.loc[index]["time"]
		if time not in used_times :
			used_times.add(time)
		else :
			df_merged.drop([index], inplace=True)
	print("重复值去除之后，共有数据数量", df_merged.shape[0])

	df_merged.set_index("time", inplace=True)
	min_time = df_merged.index.min()
	max_time = df_merged.index.max()
	min_time = datetime.datetime.strptime(min_time, '%Y-%m-%d %H:%M:%S')
	max_time = datetime.datetime.strptime(max_time, '%Y-%m-%d %H:%M:%S')
	delta_all = max_time - min_time
	print("在空气质量数据时间段内，总共应该有 %d 个小时节点。" %(delta_all.total_seconds()/3600 + 1))
	print("实际的时间节点数是 %d" %(df_merged.shape[0]))
	print("缺失时间节点数量是 %d" %(delta_all.total_seconds()/3600 + 1 - df_merged.shape[0]) )

	##个别缺失值填补
	near_stations = cal_near_stations(city)
	for index in df_merged.index :
		row = df_merged.loc[index]
		for feature in row.index :
			if pd.isnull(row[feature]):
				elements = feature.split("_")
				station_name = elements[0]
				for i in range(1,len(elements)-1):
					station_name = station_name+ "_" + elements[i]
				feature_name = elements[-1]

				temp = get_estimated_value(station_name, feature_name, near_stations, row)
				if row[feature] != 0:
					row[feature] = temp;
	
	#整小时缺失填补
	#统计缺失时间
	delta = datetime.timedelta(hours=1)
	time = min_time
	missing_hours = []
	missing_hours_str = []
	while time <=  max_time :
		if datetime.date.strftime(time, '%Y-%m-%d %H:%M:%S') not in df_merged.index :
			missing_hours.append(time)
			missing_hours_str.append(datetime.date.strftime(time, '%Y-%m-%d %H:%M:%S'))
		time += delta
	print("整小时的缺失共计 : ", len(missing_hours))

	keep_hours = []# K小时之内的填补
	drop_hours = []# K小时之外的舍弃

	delta = datetime.timedelta(hours=1)
	for hour in missing_hours_str : 
		time = datetime.datetime.strptime(hour, '%Y-%m-%d %H:%M:%S')
		# 前边第几个是非空的
		found_for = False
		i = 0
		while not found_for :
			i += 1
			for_time = time - i * delta
			for_time_str = datetime.date.strftime(for_time, '%Y-%m-%d %H:%M:%S')
			if for_time_str in df_merged.index :
				for_row = df_merged.loc[for_time_str]
				for_step = i
				found_for = True
		# 后边第几个是非空的
		found_back = False
		j = 0
		while not found_back :
			j += 1
			back_time = time + j * delta
			back_time_str = datetime.date.strftime(back_time, '%Y-%m-%d %H:%M:%S')
			if back_time_str in df_merged.index :
				back_row = df_merged.loc[back_time_str]
				back_step = j
				found_back = True

		all_steps = for_step + back_step
		if all_steps > 1 :
			drop_hours.append(hour)
		else : 
			keep_hours.append(hour)
        	# 插值
			delata_values = back_row - for_row
			df_merged.loc[hour] = for_row + (for_step/all_steps) * delata_values    

	#整小时缺失填补
	nan_series = pd.Series({key:np.nan for key in df_merged.columns})
	for hour in drop_hours:
		df_merged.loc[hour] = nan_series
	print(len(drop_hours))
	'''
	#填充NAN
	datestart= min_time
	dateend= max_time
	nan_series = pd.Series({key:np.nan for key in df_merged.columns})

	while datestart<dateend:  
		datestart+=datetime.timedelta(hours=1)
		dateTemp = datestart.strftime('%Y-%m-%d %H:%M:%S')
		if dateTemp not in used_times:
			df_merged.loc[dateTemp] = nan_series;
	'''
	print(df_merged.shape)
	df_merged.sort_index(inplace=True)
	df_merged.to_csv("preprocess/"+city+"_aq_data.csv")
	return df_merged

def load_aq_data2(city,filename=""):
	bj_aq_dataset = [];
	# load csvs 
	if filename!='':
		bj_aq_dataset = pd.read_csv(filename);
	elif city == 'bj':
		#bj_aq_dataset = pd.read_csv("download/bj_2018-04-01-002018-05-15-23_aq_data.csv")
		bj_aq_dataset = pd.read_csv("download/beijing_17_18_aq-Copy1.csv")
		'''
		bj_aq_dataset_1 = pd.read_csv("orginal_data/bj/aq/beijing_17_18_aq.csv")
		bj_aq_dataset_2 = pd.read_csv("orginal_data/bj/aq/beijing_201802_201803_aq.csv")

		#bj_aq_dataset_3 = pd.read_csv("orginal_data/Beijing/aq/new.csv")
		#bj_aq_dataset = pd.concat([bj_aq_dataset_1, bj_aq_dataset_2, bj_aq_dataset_3], ignore_index=True)
		bj_aq_dataset = pd.concat([bj_aq_dataset_1,bj_aq_dataset_2], ignore_index=True)
		'''
	elif city == 'ld':
		bj_aq_dataset  = pd.read_csv("download/London_historical_aqi_forecast_stations_20180331-Copy1.csv");
		#bj_aq_dataset = pd.read_csv("download/ld_2018-04-01-002018-05-15-23_aq_data.csv")
	else:
		return

	# turn date from string type to datetime type
	bj_aq_dataset["time"] = pd.to_datetime(bj_aq_dataset['utc_time'])
	#bj_aq_dataset.set_index("time", inplace=True)
	bj_aq_dataset.drop("utc_time", axis=1, inplace=True)
	if city == 'ld':
		bj_aq_dataset.drop("CO", axis=1, inplace=True)
		bj_aq_dataset.drop("O3", axis=1, inplace=True)
		bj_aq_dataset.drop("SO2", axis=1, inplace=True)
	# names of all stations
	stations = set(bj_aq_dataset['stationId'])
	
	# a dict of station aq, Beijing
	bj_aq_stations = {}

	bj_aq_stations_merged =[]
	for station in stations:
		#dataFrame of station
		bj_aq_station = bj_aq_dataset[bj_aq_dataset["stationId"]==station].copy()

		bj_aq_station.drop("stationId", axis=1, inplace=True)
		bj_aq_station = bj_aq_station.drop_duplicates("time")
		bj_aq_station.set_index("time", inplace=True)
		
		# rename
		original_names = bj_aq_station.columns.values.tolist()
		names_dict = {original_name : station+"_"+original_name for original_name in original_names}
		bj_aq_station_renamed = bj_aq_station.rename(index=str, columns=names_dict)
		#bj_aq_station_renamed = bj_aq_station_renamed.drop_duplicates()
		'''
		#去重
		used_times = set()
		for index in bj_aq_station_renamed.index :
			time = bj_aq_station_renamed.loc[index]["time"]
			if time not in used_times :
				used_times.add(time)
			else :
				bj_aq_station_renamed.drop([index], inplace=True)
		'''
		
		bj_aq_stations[station] = bj_aq_station_renamed
			
		
	# merge data of different stations into one df
	temp = list(bj_aq_stations.values())
	bj_aq_stations_merged = pd.concat(temp, axis=1)
	print(bj_aq_stations_merged.shape)
	# add a column of (0,1,2,3,...) to count
	length = bj_aq_stations_merged.shape[0]
	order = range(length)
	bj_aq_stations_merged['order'] = pd.Series(order, index=bj_aq_stations_merged.index)
	

	print("最早的日期：",bj_aq_stations_merged.index.min())
	print("最晚的日期：", bj_aq_stations_merged.index.max())
	#重复值为相同的数据
	df_merged = bj_aq_stations_merged
	df_merged["time"] = df_merged.index
	df_merged.set_index("order", inplace=True)
	print("重复值去除之前，共有数据数量", df_merged.shape[0])
	used_times = set()
	for index in df_merged.index :
		time = df_merged.loc[index]["time"]
		if time not in used_times :
			used_times.add(time)
		else :
			df_merged.drop([index], inplace=True)
	print("重复值去除之后，共有数据数量", df_merged.shape[0])

	df_merged.set_index("time", inplace=True)
	min_time = df_merged.index.min()
	max_time = df_merged.index.max()
	min_time = datetime.datetime.strptime(min_time, '%Y-%m-%d %H:%M:%S')
	max_time = datetime.datetime.strptime(max_time, '%Y-%m-%d %H:%M:%S')
	delta_all = max_time - min_time
	print("在空气质量数据时间段内，总共应该有 %d 个小时节点。" %(delta_all.total_seconds()/3600 + 1))
	print("实际的时间节点数是 %d" %(df_merged.shape[0]))
	print("缺失时间节点数量是 %d" %(delta_all.total_seconds()/3600 + 1 - df_merged.shape[0]) )
	

	#填补缺失时刻#
	delta = datetime.timedelta(hours=1)
	time = min_time
	nan_series = pd.Series({key:np.nan for key in df_merged.columns})

	while time <=  max_time :
		timestr = datetime.date.strftime(time, '%Y-%m-%d %H:%M:%S')
		if timestr not in df_merged.index :
			df_merged.loc[timestr]=nan_series
		time += delta

	##个别缺失值填补
	delta = datetime.timedelta(hours=1)
	for index in df_merged.index :
		print(index)
		row = df_merged.loc[index]
		time = datetime.datetime.strptime(index, '%Y-%m-%d %H:%M:%S')
		
		for feature in row.index :
			if pd.isnull(row[feature]):
				found_for = False
				for_row = ""
				for_step = 0
				i = 0
				while not found_for :
					i += 1
					for_time = time - i * delta
					if for_time < min_time:
						break;
					for_time_str = datetime.date.strftime(for_time, '%Y-%m-%d %H:%M:%S')
					if for_time_str in df_merged.index and not pd.isnull(df_merged.loc[for_time_str][feature]):
						for_row = df_merged.loc[for_time_str][feature]
						for_step = i
						found_for = True
				# 后边第几个是非空的
				found_back = False
				j = 0
				back_step=0
				back_row = ""
				while not found_back :
					j += 1
					back_time = time + j * delta
					if back_time>max_time:
						break;
					back_time_str = datetime.date.strftime(back_time, '%Y-%m-%d %H:%M:%S')
					if back_time_str in df_merged.index and not pd.isnull(df_merged.loc[back_time_str][feature]):
						back_row = df_merged.loc[back_time_str][feature]
						
						back_step = j
						found_back = True

				all_steps = for_step + back_step
				# 插值
				if found_for and found_back and all_steps<5:
					delata_values = back_row - for_row
					df_merged.loc[index][feature] = for_row + (for_step/all_steps) * delata_values
	##个别缺失值填补
	near_stations = cal_near_stations(city)
	for index in df_merged.index :
		row = df_merged.loc[index]
		for feature in row.index :
			if pd.isnull(row[feature]):
				elements = feature.split("_")
				station_name = elements[0]
				for i in range(1,len(elements)-1):
					station_name = station_name+ "_" + elements[i]
				feature_name = elements[-1]

				temp = get_estimated_value(station_name, feature_name, near_stations, row)
				if row[feature] != 0:
					row[feature] = temp;
					 
	print(df_merged.shape)
	df_merged.sort_index(inplace=True)
	df_merged.to_csv("preprocess/"+city+"_aq_dataAll.csv")
	return df_merged

#为个别缺失值填补数据
def get_estimated_value(station_name, feature_name, near_stations, row):
    '''
    为 feature 寻找合理的缺失值的替代。
    Args:
        near_stations : a dict of {station : near stations}
    '''   
    near_stations = near_stations[station_name]    # A list of nearest stations
    num = len(near_stations);
    iter = 0;

    for station in near_stations :                 # 在最近的站中依次寻找非缺失值
        feature = station + "_" +feature_name
        if not pd.isnull(row[feature]):
            return row[feature]
        iter = iter + 1;
        if iter/num >0.5:
        	break;
    return 0
#站点距离表
def cal_near_stations(city):
	aq_station_locations=[];
	sname = ""
	if city == 'bj':
		aq_station_locations = pd.read_excel("preprocess/beijing_aq_location.xlsx", sheet_name="1")
		sname = "station_name"
	elif city == 'ld':
		aq_station_locations = pd.read_excel("preprocess/London_AirQuality_Stations.xlsx", sheet_name="1")
		sname = "stationId"
	for index_t in aq_station_locations.index:
		row_t = aq_station_locations.loc[index_t]
		# location of target station
		long_t = row_t["longitude"]
		lati_t = row_t["latitude"]
		# column name
		station_name = row_t[sname]
		# add a new column to df
		all_dis = []
		for index in aq_station_locations.index:
			row = aq_station_locations.loc[index]
			long = row['longitude']
			lati = row['latitude']
			dis = np.sqrt((long-long_t)**2 + (lati-lati_t)**2)
			all_dis.append(dis)
		aq_station_locations[station_name] = all_dis
	near_stations = {}
	for index_t in aq_station_locations.index:
		target_station_name = aq_station_locations.loc[index_t][sname]
		ordered_stations_names = aq_station_locations.sort_values(by=target_station_name)[sname].values[1:]
		near_stations[target_station_name] = ordered_stations_names
	return near_stations
