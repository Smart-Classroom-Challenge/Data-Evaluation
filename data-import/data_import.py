import json
import psycopg2
import configparser

class TimescaleDB():
    def __init__(self):
        self.get_connection()

    def get_connection(self):
        config = configparser.ConfigParser()
        config.read('import-config.ini')

        db_hostname = config['DATABASE']['HOSTNAME']
        db_database = config['DATABASE']['DATABASE']
        db_username = config['DATABASE']['USERNAME']
        db_password = config['DATABASE']['PASSWORD']

        self.connection = psycopg2.connect(
            host=db_hostname,
            database=db_database,
            user=db_username,
            password=db_password)
        self.cursor = self.connection.cursor()

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
            
    def load_measurements(self, measurements_file, fk_measurement_station_id):
        with open(measurements_file, 'r') as measurements:
            line_count = 0
            for line in measurements:
                measurement = json.loads(line)
                statement = f"""INSERT INTO public.api_measurement (time,
                                                    fk_measurement_station_id,
                                                    co2,
                                                    temperature,
                                                    humidity,
                                                    insert_time,
                                                    light,
                                                    motion)
                                VALUES ('{measurement['time']}',
                                        {fk_measurement_station_id},
                                        {measurement['co2']},
                                        {measurement['temperature']},
                                        {measurement['humidity']},
                                        NOW(),
                                        {measurement['light']},
                                        '{measurement['motion']}')
                """

                try:
                    self.cursor.execute(statement)
                    line_count += 1
                    self.connection.commit() 
                except Exception as e:
                    print("Error occured at line: ", line_count, e)

            print('Successfully completed import of ', line_count, 'rows.\n')
