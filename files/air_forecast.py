!pip install requests pandas --quiet

import os
import requests
import pandas as pd
from datetime import date

cities = ["Catania", "Roma", "Bari", "Milano", "Torino"]

def geocode_city(city):
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {
        "name": city,
        "count": 1,
        "language": "it",
        "format": "json",
        "country_code": "IT"
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    
    if "results" not in data:
        raise ValueError(f"Città non trovata: {city}")
    
    res = data["results"][0]
    return res["latitude"], res["longitude"]

def fetch_realtime_weather(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "precipitation,"
            "surface_pressure,"
            "wind_speed_10m,"
            "wind_direction_10m"
        ),
        "timezone": "Europe/Rome"
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    
    df = pd.DataFrame(data["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    return df

def fetch_air_quality(lat, lon):
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm10,pm2_5,nitrogen_dioxide,ozone,carbon_monoxide,sulphur_dioxide",
        "timezone": "Europe/Rome"
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    
    df = pd.DataFrame(data["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    return df

ll_dfs = []

for city in cities:
    print(f"🔄 Scarico dati per {city}...")
    lat, lon = geocode_city(city)
    
    # Dati meteo
    df_weather = fetch_realtime_weather(lat, lon)
    
    # Dati qualità dell’aria
    df_air = fetch_air_quality(lat, lon)
    
    # Merge sui timestamp
    df_merged = pd.merge(df_weather, df_air, on="time", how="outer")
    
    # Aggiunta colonne città e coordinate
    df_merged.insert(0, "city", city)
    df_merged.insert(1, "latitude", lat)
    df_merged.insert(2, "longitude", lon)
    
    all_dfs.append(df_merged)


df_weather_all = pd.concat(all_dfs, ignore_index=True)
df_weather_all.sort_values(by=["city", "time"], inplace=True)


oggi = date.today().strftime("%Y-%m-%d")
save_dir = r"C:\Meteo1"
os.makedirs(save_dir, exist_ok=True)

output_path = os.path.join(save_dir, f"dati_meteo_qualita_aria_{oggi}.csv")
df_weather_all.to_csv(output_path, index=False, encoding="utf-8")


print(f"\n✅ Dati meteo e qualità dell’aria salvati in:\n   {output_path}")
print(f"📄 Numero totale di record: {len(df_weather_all)}")
print("\nAnteprima:")
print(df_weather_all.head(10))

