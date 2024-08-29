from datetime import datetime, timedelta
import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()

with open("data/bus_lines.txt") as f:
    bus_lines = f.read().splitlines()

avg_speed_data = []
start_date = datetime.strptime("2024-05-23 09:00:00", "%Y-%m-%d %H:%M:%S")
end_date = datetime.strptime("2024-05-23 18:00:00", "%Y-%m-%d %H:%M:%S")
token = os.environ['MOBILITY_TWIN_TOKEN']

# 10 minute interval
interval_seconds = 60 * 10
avg_speed_data = []
while start_date <= end_date:
    t = int(start_date.timestamp())
    try:
        response = requests.get(
            f"https://api.mobilitytwin.brussels/stib/aggregated-speed?timestamp={t}",
            headers={"Authorization": f"Bearer {token}"},
        )
        # drop trams and night buses
        bus_info = [entry for entry in response.json() if entry["lineId"] in bus_lines]
        avg_speed_data.append(bus_info)
    except Exception as e:
        print(response)
        print(response.json())
        print(f"An error occurred: {e}")
    else:
        start_date += timedelta(seconds=interval_seconds)
        

with open("data/agg_speed_2024-05-23_9am_6pm.json", "w") as f:
    json.dump(avg_speed_data, f, indent=4)