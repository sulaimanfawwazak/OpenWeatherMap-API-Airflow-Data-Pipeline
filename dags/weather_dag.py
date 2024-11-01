from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from dotenv import load_dotenv
from charminal import *
import json
import os
import pandas as pd

load_dotenv('/home/pwnwas/airflow/dags/.env')

BASE_DIR = '/home/pwnwas/airflow'

# Get the session token: type `aws sts get-session-token`
aws_credentials = {
  'key': os.getenv('ACCESS_KEY_ID'),
  'secret': os.getenv('SECRET_ACCESS_KEY'),
  'token': os.getenv('SESSION_TOKEN')
}

############## DATA EXAMPLE ##############
###### {
######   "coord": {
######     "lon": 106.8451,
######     "lat": -6.2146
######   },
######   "weather": [
######     {
######       "id": 721,
######       "main": "Haze",
######       "description": "haze",
######       "icon": "50d"
######     }
######   ],
######   "base": "stations",
######   "main": {
######     "temp": 307.27,
######     "feels_like": 312.16,
######     "temp_min": 306.05,
######     "temp_max": 308.44,
######     "pressure": 1011,
######     "humidity": 51,
######     "sea_level": 1011,
######     "grnd_level": 1009
######   },
######   "visibility": 5000,
######   "wind": {
######     "speed": 1.54,
######     "deg": 120
######   },
######   "clouds": {
######     "all": 20
######   },
######   "dt": 1730430534,
######   "sys": {
######     "type": 2,
######     "id": 2073276,
######     "country": "ID",
######     "sunrise": 1730413575,
######     "sunset": 1730457962
######   },
######   "timezone": 25200,
######   "id": 1642911,
######   "name": "Jakarta",
######   "cod": 200
###### }

def kelvin_to_celcius(temp):
  return temp - 273

def transform_load_data(task_instance):
  data = task_instance.xcom_pull(task_ids='extract_weather_data')
  
  # Parse the data
  city = data['name']
  weather_description = data['weather'][0]['description']
  temp_celcius = kelvin_to_celcius(data['main']['temp'])
  feels_like_celcius = kelvin_to_celcius(data['main']['feels_like'])
  min_temp_celcius = kelvin_to_celcius(data['main']['temp_min'])
  max_temp_celcius = kelvin_to_celcius(data['main']['temp_max'])
  pressure = data['main']['pressure']
  humidity = data['main']['humidity']
  wind_speed = data['wind']['speed']
  time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
  sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
  sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

  # Format the data from the parsed data
  transformed_data = {
    'City': city,
    'Description': weather_description,
    'Temperature (C)': temp_celcius,
    'Feels Like (C)': feels_like_celcius,
    'Min Temp': min_temp_celcius,
    'Max Temp': max_temp_celcius,
    'Pressure': pressure,
    'Humidity': humidity,
    'Wind Speed': wind_speed,
    'Time of Record': time_of_record,
    'Surise (Local Time)': sunrise_time,
    'Sunset (Local Time)': sunset_time
  }

  transformed_data_list = [transformed_data]
  df_data = pd.DataFrame(transformed_data_list)

  time_now = datetime.now()
  dt_string = time_now.strftime('%d%m%Y%H%M%S')
  dt_string = f'current_weather_data_jakarta_{dt_string}'
  df_data.to_csv(f'{BASE_DIR}/{dt_string}.csv', index=False)
  df_data.to_csv(f's3://{os.getenv('S3_BUCKET_NAME')}/{dt_string}.csv', index=False, storage_options=aws_credentials)
  print(f'{COLOR_GREEN}CSV file saved succesfully at {dt_string}{RESET}')

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2023, 1, 8),
  'email': ['sulaimanfawwazak@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 2,
  'retry_delay': timedelta(minutes=2) 
}

CITY = 'jakarta'
API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')

with DAG(
  'weather_dag',
  default_args=default_args,
  schedule_interval='@daily',
  catchup=False
) as dag:
  
  # 1st Task: Check if the OpenWeather API is ready
  is_weather_api_ready = HttpSensor(
    task_id='is_weather_api_ready',
    http_conn_id='openweathermap_api',
    endpoint=f'/data/2.5/weather?q={CITY}&appid={API_KEY}'
    # response_check=lambda response: response.status_code == 200,
    # poke_interval=5, # Time in seconds to wait before retrying
    # timeout=20       # Maximum time in seconds to wait for a successful response
  )

  # 2nd Task: Get weather data from the API
  extract_weather_data = SimpleHttpOperator(
    task_id='extract_weather_data',
    http_conn_id='openweathermap_api',
    endpoint=f'/data/2.5/weather?q={CITY}&appid={API_KEY}',
    method='GET',
    response_filter=lambda response: json.loads(response.text),
    log_response=True
  )

  # 3rd Task: Transform the data
  transform_load_weather_data = PythonOperator(
    task_id='transform_load_weather_data',
    python_callable=transform_load_data
  )

  # Specify the flow of the tasks
  is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
