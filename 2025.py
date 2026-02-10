from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import requests
import pandas as pd
from datetime import datetime

cities = {
    "Delhi": {"lat": 28.6139, "lon": 77.2090},
    "Jaipur": {"lat": 26.9124, "lon": 75.7873},
    "Lucknow": {"lat": 26.8467, "lon": 80.9462},
    "Ahmedabad": {"lat": 23.0225, "lon": 72.5714},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Pune": {"lat": 18.5204, "lon": 73.8567},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Chennai": {"lat": 13.0827, "lon": 80.2707},
    "Bangalore": {"lat": 12.9716, "lon": 77.5946}
}

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

START_DATE = "2025-01-01"
END_DATE   = "2025-12-31"

all_data = []


for city, coords in cities.items():
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "precipitation_sum",
            "rain_sum",
            "weathercode",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
            "wind_direction_10m_dominant"
        ],
        "timezone": "Asia/Kolkata"
    }

    print(f"Fetching data for {city}...")

    response = requests.get(ARCHIVE_URL, params=params)
    response.raise_for_status()
    daily = response.json()["daily"]

    for i in range(len(daily["time"])):
        all_data.append({
            "city": city,
            "date": daily["time"][i],
            "temperature_2m_max": daily["temperature_2m_max"][i],
            "temperature_2m_min": daily["temperature_2m_min"][i],
            "apparent_temperature_max": daily["apparent_temperature_max"][i],
            "apparent_temperature_min": daily["apparent_temperature_min"][i],
            "precipitation_sum": daily["precipitation_sum"][i],
            "rain_sum": daily["rain_sum"][i],
            "weather_code": daily["weathercode"][i],
            "wind_speed_10m_max": daily["wind_speed_10m_max"][i],
            "wind_gusts_10m_max": daily["wind_gusts_10m_max"][i],
            "wind_direction_10m_dominant": daily["wind_direction_10m_dominant"][i]
        })

df_2025_pd = pd.DataFrame(all_data)

df_2025_pd.to_csv("/home/hp/Desktop/Bigdata_project/BigData-Project/weather_dataset.csv", mode="a", index=False, header=False)
