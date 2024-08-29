import json
import geopandas as gpd
import pandas as pd
from datetime import datetime, timedelta
from shapely.geometry import LineString

from tqdm.auto import tqdm, trange

def assign_direction_id(group):
    max_stop_sequence_row = group.loc[group['stop_sequence'].idxmax()]
    group['direction_id'] = max_stop_sequence_row['stop_id']
    return group

stops = gpd.read_file('preprocessed_data/Stops.geojson')
stops = stops.groupby(['line_id', 'direction']).apply(assign_direction_id)
stops.reset_index(drop=True, inplace=True)

with open("data/bus_lines.txt") as f:
    lines = f.read().splitlines()

with open("data/vehicle_positions_2024-08-27_09:02:21.json") as f:
    vehicle_positions = json.load(f)

for line in vehicle_positions:
    for instance in vehicle_positions[line][:]:
        if instance['vehicle_positions'] is None:
            vehicle_positions[line].remove(instance)

def reverse_coordinates(geometry):
    if geometry.geom_type == 'LineString':
        # Reverse the coordinates of each point in the LineString
        reversed_coords = [(y, x) for x, y in geometry.coords]
        return LineString(reversed_coords)
    else:
        # Handle other geometry types if necessary
        return geometry

segments = gpd.read_file('data/segments.geojson')
segments['geometry'] = segments['geometry'].apply(reverse_coordinates)
segments = segments.to_crs(epsg=3812)
segments = segments.drop(columns=['color'])
segments['line_id'] = segments['line_id'].astype(int)
segments['segment_length'] = segments['geometry'].length
segments = segments.drop(columns=['id', 'distance', 'direction'])

def filter_zero_distance(instance):
    if len(instance['vehicle_positions']) == 0:
        return instance
    vehicle_positions = instance['vehicle_positions']
    instance['vehicle_positions'] = [vehicle for vehicle in vehicle_positions if vehicle['distanceFromPoint'] > 0]
    return instance

def round_to_nearest_10_minutes(dt):
    """
    Rounds a datetime object to the nearest 10 minutes.

    :param dt: datetime object to be rounded
    :return: datetime object rounded to the nearest 10 minutes
    """
    # Calculate the number of minutes to add or subtract to round to the nearest 10 minutes
    new_minute = (dt.minute // 10) * 10
    remainder = dt.minute % 10

    if remainder >= 5:
        # If the remainder is 5 or more, round up to the next 10 minutes
        dt = dt.replace(minute=new_minute, second=0, microsecond=0) + timedelta(minutes=10)
    else:
        # Otherwise, round down to the nearest 10 minutes
        dt = dt.replace(minute=new_minute, second=0, microsecond=0)
    dt = dt.replace(second=0, microsecond=0)
    return dt

def merge_with_flexible_direction(df, stops_line):
    """
    Merge two DataFrames allowing for slight discrepancies in the 'directionId' column.
    
    Parameters:
    df (pd.DataFrame): The first DataFrame containing 'pointId' and 'directionId'.
    stops_line (pd.DataFrame): The second DataFrame containing 'stop_id', 'direction_id', 'stop_name', and 'stop_sequence'.
    
    Returns:
    pd.DataFrame: The merged DataFrame with a flexible match on 'directionId'.
    """
    
    # Original merge
    df_merged = df.merge(
        stops_line[["stop_id", "direction_id", "stop_name", "stop_sequence"]],
        left_on=["pointId", "directionId"],
        right_on=["stop_id", "direction_id"],
        how="inner",
    )

    # Merge with directionId + 1
    df_merged_plus1 = df.merge(
        stops_line[["stop_id", "direction_id", "stop_name", "stop_sequence"]],
        left_on=["pointId", df["directionId"] + 1],
        right_on=["stop_id", "direction_id"],
        how="inner",
    )

    # Merge with directionId - 1
    df_merged_minus1 = df.merge(
        stops_line[["stop_id", "direction_id", "stop_name", "stop_sequence"]],
        left_on=["pointId", df["directionId"] - 1],
        right_on=["stop_id", "direction_id"],
        how="inner",
    )

    # Concatenate the results
    df_final = pd.concat([df_merged, df_merged_plus1, df_merged_minus1]).drop_duplicates()

    # Drop the "stop_id" column if needed
    df_final = df_final.drop(columns=["stop_id"])

    df_final.reset_index(drop=True, inplace=True)
    
    return df_final

def to_df(instance, line_id):
    line_id = int(line_id)
    df = pd.DataFrame(instance["vehicle_positions"])
    df["directionId"] = df["directionId"].astype(int)
    df["pointId"] = df["pointId"].astype(int)
    # filter by line_id
    stops_line = stops[stops["line_id"] == line_id]
    segments_line = segments[segments["line_id"] == line_id]
    # for cases like bus 71, where the directionId in data is +/-1 the direction_id in stops
    num_rows_orig = df.shape[0]
    df = merge_with_flexible_direction(df, stops_line)
    num_rows_removed = num_rows_orig - df.shape[0]
    df = df.merge(
        segments_line[["start", "end", "segment_length"]],
        left_on="pointId",
        right_on="start",
        how="inner",
    ).drop(columns=["start"])
    df["rank"] = df.groupby(["directionId", "pointId"])["distanceFromPoint"].rank()
    df["rank"] = df["rank"].astype(int)
    return df, num_rows_removed


def join_dataframes(df1, df2):
    # Step 1: Perform exact match on directionId, stop_sequence, rank, and distanceFromPoint condition
    # This means that the bus is in the same stop
    exact_match = pd.merge(
        df1,
        df2,
        on=["pointId", "directionId", "stop_sequence", "rank"],
        how="left",
        suffixes=("_df1", "_df2"),
    )
    distance_from_point_condition = exact_match['distanceFromPoint_df2'] > exact_match['distanceFromPoint_df1']
    # set row to null to where this is false
    exact_match.loc[~distance_from_point_condition] = pd.NA
    # add pointId_df1 and pointId_df2 columns to store the pointId values for both instances (they are the same)
    exact_match.loc[~exact_match['stop_name_df2'].isna(), 'pointId_df1'] = exact_match.loc[~exact_match['stop_name_df2'].isna(), 'pointId']
    exact_match.loc[~exact_match['stop_name_df2'].isna(), 'pointId_df2'] = exact_match.loc[~exact_match['stop_name_df2'].isna(), 'pointId']
    # drop the pointId column since it is redundant
    exact_match.drop(columns=['pointId'], inplace=True)

    # Step 2: Perform match on directionId and (df1.stop_sequence + 1 = df2.stop_sequence)
    df1_shifted = df1.copy()
    df1_shifted["stop_sequence"] += 1
    shifted_match = pd.merge(
        df1_shifted,
        df2,
        on=["directionId", "stop_sequence", "rank"],
        how="left",
        suffixes=("_df1", "_df2"),
    )

    # Step 3: Combine results, prioritizing the exact match
    combined = exact_match.combine_first(shifted_match)
    
    # Step 4: Drop any duplicates and keep the first occurrence based on original df1
    combined = combined.drop_duplicates(
        subset=["directionId", "stop_sequence", "rank"], keep="first"
    )

    # Step 5: Clean up the final DataFrame
    final_result = combined.dropna()

    return final_result


def get_interval_pair(i, line_id):
    instance1 = vehicle_positions[str(line_id)][i]
    instance1 = filter_zero_distance(instance1)
    num_rows_instance = len(instance1["vehicle_positions"])
    
    instance2 = vehicle_positions[str(line_id)][i + 1]
    instance2 = filter_zero_distance(instance2)
    
    ts1 = datetime.strptime(instance1["timestamp"], "%Y-%m-%d %H:%M:%S")
    ts2 = datetime.strptime(instance2["timestamp"], "%Y-%m-%d %H:%M:%S")
    interval_in_seconds = (ts2 - ts1).total_seconds()

    if (
        len(instance1["vehicle_positions"]) == 0
        or len(instance2["vehicle_positions"]) == 0
    ):
        return None

    df1, num_rows_removed = to_df(instance1, line_id)
    df2, _ = to_df(instance2, line_id)
    df3 = join_dataframes(df1, df2)

    condition = df3["pointId_df1"] == df3["pointId_df2"]

    # Calculate distance_traveled based on the condition
    df3["distance_traveled"] = condition * (
        df3["distanceFromPoint_df2"] - df3["distanceFromPoint_df1"]
    ) + (~condition) * (
        (df3["segment_length_df1"] - df3["distanceFromPoint_df1"])
        + df3["distanceFromPoint_df2"]
    )
    # if the time interval is greater than 40 seconds, the distance traveled is 0
    # because we missed a data point, so the time interval is unknown
    if interval_in_seconds >= 40:
        df3["distance_traveled"] = 0
    df3["speed"] = df3["distance_traveled"] / 20
    df3["timestamp"] = ts1

    df3 = df3[
        [
            "directionId",
            "pointId_df1",
            "end_df1",
            "distanceFromPoint_df1",
            "stop_name_df1",
            "segment_length_df1",
            "distanceFromPoint_df2",
            "stop_name_df2",
            "segment_length_df2",
            "distance_traveled",
            "speed",
            "timestamp"
        ]
    ]

    return {
        "df1": df1,
        "df2": df2,
        "df3": df3,
        "num_rows_instance": num_rows_instance,
        "num_rows_removed": num_rows_removed
    }


def get_interval_pair_all_lines_all_instances(apply_filter=True):
    total_df = None
    total_rows_per_instance = 0
    total_rows_removed = 0
    for line in tqdm(vehicle_positions):
        vehicle_positions_line = vehicle_positions[line]
        line_id = int(line)
        for i in trange(len(vehicle_positions_line) - 1):
            result = get_interval_pair(i, line_id)
            if result is None:
                continue
            df3 = result["df3"]
            total_rows_per_instance += result["num_rows_instance"]
            total_rows_removed += result["num_rows_removed"]
            if total_df is None:
                total_df = df3
            else:
                if apply_filter:
                    df3 = df3[(df3["speed"] > 0) & (df3["speed"] < 25)]
                total_df = pd.concat([total_df, df3])
    return total_df, total_rows_removed / total_rows_per_instance * 100

df_all_intervals, perc_removed = get_interval_pair_all_lines_all_instances(apply_filter=True)
print("Percentage of data removed due to deviations:", perc_removed)

df_all_intervals.rename(columns={
    "pointId_df1": "start",
    "end_df1": "end",
}, inplace=True)
df_all_intervals['start'] = df_all_intervals['start'].astype(int)
df_all_intervals['end'] = df_all_intervals['end'].astype(int)
df_all_intervals['timestamp'] = pd.to_datetime(df_all_intervals['timestamp'])

# Round 'timestamp' to the nearest 10 minutes
df_all_intervals['datetime'] = df_all_intervals['timestamp'].dt.floor('10T')

# Compute the mean and median speed per segment and interval
grouped = df_all_intervals.groupby(['start', 'end', 'datetime'])
df_10m_intervals = grouped['speed'].agg(['mean', 'median']).reset_index()
df_10m_intervals.to_csv('data/speeds_stib.csv', index=False)