import requests
from tqdm.auto import tqdm
from dotenv import load_dotenv

import time
import os
import json
from ast import literal_eval    
from pprint import pprint
from datetime import datetime, timedelta
from collections import defaultdict
from tqdm.auto import tqdm

with open("../data/bus_lines.txt") as f:
    bus_lines = f.read().splitlines()

load_dotenv()

api_key = os.environ['STIB_API_KEY']
headers = {
    "Authorization": f"Apikey {api_key}"
}
url = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/vehicle-position-rt-production/records"

session = requests.Session()
session.headers.update(headers)

with open("../data/bus_lines.txt") as f:
    bus_lines = f.read().splitlines()

params = {
    'timezone': 'Europe/Brussels',
}

def get_data(params={}):
    response = session.get(url, params=params)
    data = response.json()
    if response.status_code != 200:
        pprint(data)
        raise Exception(f"Error: {response.status_code}")
    else:
        data = response.json()['results']
        for i, entry in enumerate(data):
            data[i]['vehiclepositions'] = literal_eval(entry['vehiclepositions'])
        return data

def collect_data_until(dt_until):
    segments_data = defaultdict(list)

    dt_now = datetime.now()
    dt_init = dt_now.strftime("%Y-%m-%d_%H:%M:%S")

    i = 0
    total = (dt_until - dt_now).seconds // 13

    # Initialize the progress bar
    with tqdm(
        total=total,
        desc="Collecting vehicle positions",
        unit="iteration",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
    ) as pbar:
        while dt_now < dt_until:
            dt_now = datetime.now()
            # print(
            #     f"Collecting vehicle positions at {dt_now.strftime('%H:%M:%S')} ({i}/{total})"
            # )

            for line in range(0, len(bus_lines), 10):
                required_lines = bus_lines[line : line + 10]
                params["where"] = f"lineid in {str(tuple(required_lines))}"
                vehicle_positions = get_data(params)
                for line in vehicle_positions:
                    vehicle_positions_with_timestamp = {
                        "timestamp": dt_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "vehicle_positions": line["vehiclepositions"],
                    }
                    if (
                        vehicle_positions_with_timestamp
                        not in segments_data[line["lineid"]]
                    ):
                        segments_data[line["lineid"]].append(
                            vehicle_positions_with_timestamp
                        )

            i += 1
            with open(f"vehicle_positions_{dt_init}.json", "w") as f:
                json.dump(segments_data, f, indent=2)

            # Update progress bar
            pbar.update(1)

            # 13 seconds interval
            if dt_now + timedelta(seconds=13) <= dt_until:
                time.sleep(13)
                dt_now = datetime.now()
            else:
                break

    return segments_data

H = 8
M = 0

dt = datetime.now() + timedelta(hours=H, minutes=M)
datetime.strftime(dt, '%Y-%m-%d %H:%M:%S')

dt_now = datetime.now()
dt_later = dt_now + timedelta(hours=H, minutes=M)
data = collect_data_until(dt_later)




