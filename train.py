import numpy as np
from sklearn.ensemble import RandomForestRegressor as RFR
# from xgboost import XGBRegressor as XGBR
import utils

MAX_DEPTH = 30
STATION_NUM = {'bj': 35, 'ld': 13}
HOUR_NUM = 24

if __name__ == '__main__':
    duration = '2018-04-18-0/2018-04-23-23'
    city = 'bj'
    url_reader = utils.UrlReader(duration)
    url_data = url_reader.read_with_time(city=city)
    x_train_data = np.array([url_data['station'], url_data['day'], url_data['hour']]).T
    y_train_data = np.array([url_data['pm25'], url_data['pm10'], url_data['o3']]).T

    station_list = []
    for station in range(STATION_NUM[city]):
        station_list = np.concatenate((station_list, [station] * HOUR_NUM))
    day_delta_list = [url_data['day_delta']] * HOUR_NUM * STATION_NUM[city]
    hour_list = list(range(HOUR_NUM)) * STATION_NUM[city]
    x_predict_data = np.array([station_list, day_delta_list, hour_list]).T

    regr_rf = RFR(max_depth=MAX_DEPTH, random_state=2)
    regr_rf.fit(x_train_data, y_train_data)

    y_rf = regr_rf.predict(x_predict_data)
    filename = 'sub-bj.csv'
    csv_saver = utils.CsvSaver(x_predict_data, y_rf, city, filename)
    csv_saver.save()
