# obtain data from segments or stops

from datetime import datetime
import requests
import json
from tqdm.auto import tqdm
import os
from dotenv import load_dotenv

with open("data/bus_lines.txt") as f:
    bus_lines = f.read().splitlines()

load_dotenv()
start_date = datetime.strptime("2024-05-23 09:00:00", "%Y-%m-%d %H:%M:%S")
t = int(start_date.timestamp())
token = os.environ['MOBILITY_TWIN_TOKEN']

url = f"https://api.mobilitytwin.brussels/stib/segments?timestamp={t}"
r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
data = r.json()

# data["features"] = list(filter(lambda x: x["properties"]["route_short_name"] in bus_lines, data["features"]))

with open("data/segments.geojson", "w") as f:
    json.dump(data, f, indent=2)