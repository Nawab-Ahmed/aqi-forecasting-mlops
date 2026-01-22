import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_weather_data(city):
    """Fetch current weather features from OpenWeather."""
    api_key = os.getenv("OPENWEATHER_API_KEY")
    # Using 'units=metric' for Celsius and m/s
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            'temp': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'wind_speed': data['wind']['speed']
        }
    else:
        print(f"Weather API Error: {response.status_code}")
        return None

def get_aqi_data(city):
    """Fetch AQI and pollutants from AQICN."""
    token = os.getenv("AQICN_TOKEN")
    url = f"https://api.waqi.info/feed/{city}/?token={token}"
    
    response = requests.get(url)
    data = response.json()
    if data['status'] == 'ok':
        iaqi = data['data']['iaqi']
        return {
            'aqi': data['data']['aqi'],
            'pm25': iaqi.get('pm25', {}).get('v'),
            'pm10': iaqi.get('pm10', {}).get('v')
        }
    return None

def collect_all_features(city):
    """Combine weather and AQI into a single record."""
    weather = get_weather_data(city)
    aqi = get_aqi_data(city)
    
    if weather and aqi:
        # Merge dictionaries and add metadata
        record = {**weather, **aqi}
        record['city'] = city.lower()
        # Create a Unix timestamp in milliseconds (Hopsworks requirement)
        record['date'] = int(datetime.now().timestamp() * 1000)
        
        df = pd.DataFrame([record])
        return df
    return None

if __name__ == "__main__":
    city = "Karachi" # You can use your city!
    data = collect_all_features(city)
    print("\n--- Consolidated Feature Record ---")
    print(data)