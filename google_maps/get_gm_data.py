import json
from datetime import datetime, timedelta
from pprint import pprint
import os
import pickle

import requests
from dotenv import load_dotenv, find_dotenv
import geopandas as gpd
import pytz
from tqdm.auto import trange

load_dotenv()

google_maps_api_key = os.environ["GOOGLE_MAPS_API_KEY2"]

session = requests.Session()
headers = {
    "Content-Type": "application/json",
    "X-Goog-Api-Key": google_maps_api_key,
    "X-Goog-FieldMask": "routes.legs.distanceMeters,routes.legs.duration",
}
session.headers.update(headers)

# Function to get the RFC 3339 formatted time for a given timezone
def get_rfc3339_time(
    hours_from_now=0, minutes_from_now=0, timezone_str="Europe/Brussels"
):
    tz = pytz.timezone(timezone_str)
    future_time = datetime.now(tz) + timedelta(
        hours=hours_from_now, minutes=minutes_from_now
    )
    return future_time.isoformat()

gdf = gpd.read_file("data/segments.geojson", driver="GeoJSON")
gdf = gdf[["line_id", "direction", "geometry"]]
gdf["line_id"] = gdf["line_id"].astype(int)

def get_locations_data(gdf):
    locations = []
    # obtain coordinates for each stop in the line
    for _, row in gdf.iterrows():
        coords = row.geometry.coords[0]
        lon, lat = coords
        location_obj = {"location": {"latLng": {"latitude": lat, "longitude": lon}}}
        locations.append(location_obj)

    # the final coords are the last point in the last stop
    final_coords = row.geometry.coords[-1]
    lon, lat = final_coords
    location_obj = {"location": {"latLng": {"latitude": lat, "longitude": lon}}}
    locations.append(location_obj)

    return locations

def get_speed_data_routes_api(gdf, time):
    url = "https://routes.googleapis.com/directions/v2:computeRoutes"
    locations = get_locations_data(gdf)
    total_data = []

    for i in range(0, len(locations), 25):
        if i == 0:
            locations_list = locations[i : i + 25]
        else:
            locations_list = locations[i - 1 : i + 25]

        payload = {
            "origin": locations_list[0],
            "destination": locations_list[-1],
            "intermediates": locations_list[1:-1],
            "routingPreference": "TRAFFIC_AWARE_OPTIMAL",
            "computeAlternativeRoutes": False,
            "routeModifiers": {
                "avoidTolls": False,
                "avoidHighways": False,
                "avoidFerries": False,
            },
            "departureTime": time,
            "languageCode": "en-US",
            "units": "METRIC",
        }

        response = session.post(url, data=json.dumps(payload))
        # Check the response
        if response.status_code == 200:
            data = response.json()
            if len(locations_list) > 1:
                assert len(data["routes"][0]["legs"]) + 1 == len(
                    locations_list
                ), f"A route was skipped ({len(data['routes'][0]['legs']) + 1} != {len(locations_list)})"
            data = data["routes"][0]["legs"]
            total_data.extend(data)
        else:
            pprint(response.json())
            raise Exception(f"Request failed with status code: {response.status_code}")
    return total_data

def collect_google_maps_data(gdf, save_filename, H=8, interval=10, debug=False):
    grouped = gdf.groupby(["line_id", "direction"])

    if os.path.isfile(save_filename):
        with open(save_filename, "rb") as f:
            segment_speeds = pickle.load(f)
    else:
        segment_speeds = {}

    # 2. Iterate over each group
    for hour in trange(H):
        for minute in range(0, 60, interval):
            for (line_id, direction), group in grouped:
                # only future data allowed
                if hour == 0 and minute == 0:
                    minute += 1
                # do not recollect data for segments
                if (line_id, direction, hour, minute) in segment_speeds:
                    continue
                # convert current datetime to iso format
                time = get_rfc3339_time(hours_from_now=hour, minutes_from_now=minute)
                speed_data = get_speed_data_routes_api(group, time)
                if speed_data is None:
                    raise Exception("Stopped due to error")
                else:
                    for i, estimates in enumerate(speed_data):
                        duration = int(estimates["duration"][:-1])
                        if duration == 0:
                            speed_data[i]["distanceMeters"] = 0
                            speed_data[i]["speed"] = 0
                        else:
                            speed_data[i]["speed"] = (
                                estimates["distanceMeters"] / duration
                            )
                    segment_speeds[(line_id, direction, hour, minute)] = {
                        "time": time,
                        "speed_data": speed_data,
                    }
                    if debug:
                        print(
                            f"Processed segment for line_id: {line_id}, direction: {direction}, hours: {hour}, minutes: {minute}"
                        )
    else:
        for (line_id, direction), group in grouped:
            # finished successfully, so get data for last hour
            time = get_rfc3339_time(hours_from_now=H)
            speed_data = get_speed_data_routes_api(group, time)
            if speed_data is None:
                raise Exception("Stopped due to error")
            else:
                for i, estimates in enumerate(speed_data):
                    duration = int(estimates["duration"][:-1])
                    if duration == 0:
                        speed_data[i]["distanceMeters"] = 0
                        speed_data[i]["speed"] = 0
                    else:
                        speed_data[i]["speed"] = estimates["distanceMeters"] / duration
                segment_speeds[(line_id, direction, H, 0)] = {
                    "time": time,
                    "speed_data": speed_data,
                }
                if debug:
                    print(
                        f"Processed segment for line_id: {line_id}, direction: {direction}, hours: {H}, minutes: 0"
                    )

    with open(save_filename, "wb") as f:
        pickle.dump(segment_speeds, f)

    return segment_speeds

H = 8
interval = 10
save_filename = "data/gm_segment_speeds_aug27.pkl"
data = collect_google_maps_data(gdf, H=H, interval=interval)