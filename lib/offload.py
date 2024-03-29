import pytz
import psycopg2
import configparser
import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine


class Offload:
    plt.rcParams['timezone'] = "CET"
    meteofile = "data/2022_05_17_meteoswiss.txt"

    def __init__(self):
        pass

    def __read_config(self, filename='config.ini'):
        config = configparser.ConfigParser()
        config.read(filename)

        db_hostname = config['DATABASE']['HOSTNAME']
        db_database = config['DATABASE']['DATABASE']
        db_username = config['DATABASE']['USERNAME']
        db_password = config['DATABASE']['PASSWORD']

        return db_hostname, db_database, db_username, db_password

    def __get_connection(self):
        db_hostname, db_database, db_username, db_password = self.__read_config()
        return create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_hostname}/{db_database}').connect()

    def get_measurement(self, name, startDate, endDate):
        con = self.__get_connection()

        sql_stations = f"""
            set timezone = 'CET'; 
            select * from api_classroom
            inner join api_measurementstation on api_classroom.id = api_measurementstation.fk_classroom_id
            inner join api_measurement on api_measurementstation.id = api_measurement.fk_measurement_station_id
            where api_classroom.name = '{name}' and time BETWEEN '{startDate}' AND '{endDate}'
            order by time;
        """
        result = pd.read_sql_query(sql_stations, con)

        if not result.empty:
            result["time"] = result["time"].dt.tz_convert("CET")
            result["updated_on"] = result["updated_on"].dt.tz_convert("CET")
            result["insert_time"] = result["insert_time"].dt.tz_convert("CET")

        con.close()
        return result

    def get_singe_measurement(self, name, datetime):
        con = self.__get_connection()

        sql_stations = f"""
            set timezone = 'CET'; 
            select * from api_classroom
            inner join api_measurementstation on api_classroom.id = api_measurementstation.fk_classroom_id
            inner join api_measurement on api_measurementstation.id = api_measurement.fk_measurement_station_id
            where api_classroom.name = '{name}' and time = '2022-05-04 08:06:57.554084+02:00';
        """

        result = pd.read_sql_query(self, sql_stations, con)

        if not result.empty:
            result["time"] = result["time"].dt.tz_convert("CET")
            result["updated_on"] = result["updated_on"].dt.tz_convert("CET")
            result["insert_time"] = result["insert_time"].dt.tz_convert("CET")

        con.close()
        return result

    def get_measurement_for_timetable(self, name, stundenplan):
        """
        stundenplan: ist eine Liste mit einem dict zu jeder lektion, darin ist der start, ende und die personenanzahl enthalten.
        """
        start_date = datetime(2022, 4, 25)
        dfs = []

        lesson_uuid = 0
        for week in range(3):
            for lektion in stundenplan:
                date = start_date + timedelta(days=7 * week + lektion[0])
                start_time = date.replace(hour=lektion[1][0], minute=lektion[1][1])
                end_time = date.replace(hour=lektion[2][0], minute=lektion[2][1])
                df = self.get_measurement(name, start_time, end_time)
                if not df.empty:
                    df['lesson_uuid'] = lesson_uuid
                    df['People'] = lektion[3]
                    dfs.append(df)
                    lesson_uuid += 1

        return pd.concat(dfs)

    def get_timebuckets_with_diff(self, name, start_time, end_time, parameter="co2", bucket_minutes=5):
        con = self.__get_connection()

        # removed CET conversion in SQL -> no effect?
        sql_timebuckets = f"""
            SELECT ts,
                {parameter}_mean,
                {parameter}_mean - LAG({parameter}_mean) OVER (ORDER BY ts)     AS {parameter}_mean_diff,
                {parameter}_median,
                {parameter}_median - LAG({parameter}_median) OVER (ORDER BY ts) AS {parameter}_median_diff
            FROM (
                    SELECT time_bucket('{bucket_minutes} minutes', time)             AS ts,
                            round(avg({parameter}), 2)                               AS {parameter}_mean,
                            percentile_cont(0.5) WITHIN GROUP (ORDER BY {parameter}) AS {parameter}_median
                    FROM api_measurement
                            INNER JOIN api_measurementstation
                                        ON api_measurementstation.id = api_measurement.fk_measurement_station_id
                            INNER JOIN api_classroom ON api_classroom.id = api_measurementstation.fk_classroom_id
                    WHERE api_classroom.name = '{name}' and time BETWEEN '{start_time}' AND '{end_time}'
                    GROUP BY ts
                    ORDER BY ts
                ) buckets;
        """

        result = pd.read_sql_query(sql_timebuckets, con)

        if not result.empty:
            result["ts"] = result["ts"].dt.tz_convert("CET")  # returned from DB as UTC -> convert to CET

        con.close()
        return result

    def get_timebuckets_for_timetable(self, name, stundenplan):
        """
        stundenplan: ist eine Liste mit einem dict zu jeder lektion, darin ist der start, ende und die personenanzahl enthalten.
        """
        start_date = datetime(2022, 4, 25)
        dfs = []

        lesson_uuid = 0
        for week in range(3):
            for lektion in stundenplan:
                date = start_date + timedelta(days=7 * week + lektion[0])
                tz = pytz.timezone('Europe/Zurich')  # timezone aware dates for query
                start_time = tz.localize(date.replace(hour=lektion[1][0], minute=lektion[1][1]))
                end_time = tz.localize(date.replace(hour=lektion[2][0], minute=lektion[2][1]))
                df = self.get_timebuckets_with_diff(name, start_time, end_time)
                if not df.empty:
                    df['lesson_uuid'] = lesson_uuid
                    df['people'] = lektion[3]
                    dfs.append(df)
                    lesson_uuid += 1

        return pd.concat(dfs)

    def get_entrance(self, name, startDate, endDate):
        con = self.__get_connection()

        sql_stations = f"""
            set timezone = 'CET'; 
            select * from api_classroom
            inner join api_measurementstation on api_classroom.id = api_measurementstation.fk_classroom_id
            inner join api_entranceevent on api_measurementstation.id = api_entranceevent.fk_measurement_station_id
            where time BETWEEN '{startDate}' AND '{endDate}' and api_classroom.name = '{name}';
        """
        result = pd.read_sql_query(sql_stations, con)

        result["time"] = result["time"].dt.tz_convert("CET")
        result["updated_on"] = result["updated_on"].dt.tz_convert("CET")
        result["insert_time"] = result["insert_time"].dt.tz_convert("CET")

        con.close()

        result = result.sort_values(['time'])
        return result

    def get_meteo(self, startDate, endDate):
        meteo = pd.read_csv(self.meteofile, delimiter=';')
        meteo.columns = ["Station",
                         "Time",
                         "Lufttemperatur 2 m über Boden; Momentanwert in °C",
                         "Niederschlag; gleitende Stundensumme (über 6 Zehnminutenintervalle) in mm",
                         "Relative Luftfeuchtigkeit 2 m über Boden; Momentanwert in %",
                         "Windgeschwindigkeit; Zehnminutenmittel in %"]

        meteo["Time"] = pd.to_datetime(meteo["Time"], format="%Y%m%d%H%M")
        meteo["Time"] = meteo["Time"].dt.tz_localize("CET")
        meteo = meteo.set_index("Time")

        if startDate != None:
            meteo = meteo[meteo.index >= startDate]

        if endDate != None:
            meteo = meteo.loc[meteo.index <= endDate]

        return meteo

    def get_people_count_overtime(self, entrance_list):
        people_count = 0
        lst_people_count = []
        lst_time = []
        array = []

        for index, row in entrance_list.iterrows():
            people_count += row['change']
            ttime = row['time']
            lst_people_count.append(people_count)
            lst_time.append(ttime)

            d = dict({'people_count': people_count, 'time': ttime})
            array.append(d)

        df = pd.DataFrame.from_dict(array, orient='columns')
        df = df.sort_values(['time'], ascending=True)
        return df

    timetables = {
        'Primar_EG': [
            # week offset, (hour_start, minute_start), (hour_start, minute_start), people
            (0, (7, 35), (8, 00), 21),
            (0, (8, 5), (8, 50), 21),
            (0, (8, 55), (9, 40), 21),
            (0, (10, 00), (10, 45), 21),
            (0, (10, 50), (11, 35), 21),

            (1, (7, 35), (8, 00), 21),
            (1, (8, 5), (8, 50), 21),
            (1, (8, 55), (9, 40), 21),
            (1, (10, 00), (10, 45), 21),
            (1, (13, 30), (14, 15), 11),
            (1, (14, 20), (15, 5), 11),
            (1, (15, 20), (16, 5), 11),

            (2, (7, 35), (8, 00), 21),
            (2, (8, 5), (8, 50), 21),
            (2, (8, 55), (9, 40), 21),
            (2, (10, 00), (10, 45), 21),
            (2, (10, 50), (11, 35), 21),

            (3, (7, 35), (8, 00), 21),
            (3, (8, 5), (8, 50), 11),
            (3, (8, 55), (9, 40), 11),
            (3, (10, 00), (10, 45), 11),
            (3, (10, 50), (11, 35), 11),
            (3, (13, 30), (14, 15), 11),
            (3, (14, 20), (15, 5), 11),
            (3, (15, 20), (16, 5), 11),

            (4, (7, 35), (8, 00), 21),
            (4, (8, 5), (8, 50), 21),
            (4, (10, 00), (10, 45), 21),
            (4, (10, 50), (11, 35), 21),
        ],
        'Primar_OG1': [
            # week offset, (hour_start, minute_start), (hour_start, minute_start), people
            (0, (7, 35), (8, 00), 22),
            (0, (8, 5), (8, 50), 22),
            (0, (8, 55), (9, 40), 22),
            (0, (13, 30), (14, 15), 11),
            (0, (14, 20), (15, 5), 11),
            (0, (15, 20), (16, 5), 11),

            (1, (7, 35), (8, 00), 22),
            (1, (8, 5), (8, 50), 22),
            (1, (8, 55), (9, 40), 22),
            (1, (10, 00), (10, 45), 22),
            (1, (10, 50), (11, 35), 22),
            (1, (13, 30), (14, 15), 11),
            (1, (14, 20), (15, 5), 11),
            (1, (15, 20), (16, 5), 11),

            (2, (7, 35), (8, 00), 22),
            (2, (8, 5), (8, 50), 22),
            (2, (10, 00), (10, 45), 11),
            (2, (10, 50), (11, 35), 11),

            (3, (7, 35), (8, 00), 22),
            (3, (8, 5), (8, 50), 22),
            (3, (8, 55), (9, 40), 22),
            (3, (10, 00), (10, 45), 22),
            (3, (13, 30), (14, 15), 22),
            (3, (14, 20), (15, 5), 22),
            (3, (15, 20), (16, 5), 22),

            (4, (7, 35), (8, 00), 22),
            (4, (8, 5), (8, 50), 11),
            (4, (8, 55), (9, 40), 11),
            (4, (10, 00), (10, 45), 11),
            (4, (10, 50), (11, 35), 11),
        ],
        'Oberstufe': [
            (0, (8, 55), (9, 40), 10),
            (0, (10, 00), (10, 45), 20),
            (0, (10, 50), (11, 35), 20),

            (1, (8, 5), (8, 50), 16),
            (1, (8, 55), (9, 40), 10),
            (1, (10, 00), (10, 45), 10),
            (1, (10, 50), (11, 35), 21),
            (1, (12, 30), (13, 15), 10),

            (2, (8, 5), (8, 50), 20),
            (2, (8, 55), (9, 40), 10),
            (2, (10, 00), (10, 45), 16),
            (2, (10, 50), (11, 35), 16),

            (3, (7, 15), (8, 00), 20),
            (3, (8, 5), (8, 50), 20),
            (3, (10, 00), (10, 45), 13),
            (3, (10, 50), (11, 35), 13),
            (3, (13, 30), (14, 15), 16),
            (3, (14, 20), (15, 5), 16),

            (4, (12, 30), (13, 15), 12),
            (4, (13, 30), (14, 15), 13),
            (4, (14, 20), (15, 5), 10),
            (4, (15, 20), (16, 5), 14),
        ]
    }
