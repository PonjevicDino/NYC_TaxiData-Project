import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

dataset_root = "./Dataset_Modified"
output_file = './RQ2.parquet'

all_pairs = []
all_start_zones = []

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print("Processing: " + parquet_file.name)

    df = pq.read_table(parquet_file).to_pandas()

    zone_combinations = df.groupby(['Start_Zone', 'End_Zone']).agg(
        Count=('Trip_Distance', 'size'),
        Sum_Distance=('Trip_Distance', 'sum')
    ).reset_index()
    all_pairs.append(zone_combinations)

    start_zone_group = df.groupby('Start_Zone').agg(
        Total_Count=('Trip_Distance', 'size'),
        Total_Sum=('Trip_Distance', 'sum')
    ).reset_index()
    all_start_zones.append(start_zone_group)

zone_pairs = pd.concat(all_pairs, ignore_index=True)
final_pairs = zone_pairs.groupby(['Start_Zone', 'End_Zone']).agg({
    'Count': 'sum',
    'Sum_Distance': 'sum'
}).reset_index()

final_pairs['Avg_Distance'] = final_pairs['Sum_Distance'] / final_pairs['Count']
index = final_pairs.groupby('Start_Zone')['Count'].idxmax()
favorite_zones = final_pairs.loc[index, ['Start_Zone', 'End_Zone', 'Avg_Distance']]
favorite_zones = favorite_zones.rename(columns={'End_Zone': 'Favorite_End_Zone'})

combined_start = pd.concat(all_start_zones, ignore_index=True)
start_zone_avg = combined_start.groupby('Start_Zone').agg({
    'Total_Count': 'sum',
    'Total_Sum': 'sum'
}).reset_index()
start_zone_avg['Avg_Total_Distance'] = start_zone_avg['Total_Sum'] / start_zone_avg['Total_Count']

final_result = pd.merge(
    favorite_zones,
    start_zone_avg[['Start_Zone', 'Avg_Total_Distance']],
    on='Start_Zone',
    how='left'
)

final_table = pa.Table.from_pandas(final_result, preserve_index=False)

try:
    pq.write_table(final_table, output_file_name)
except Exception as e:
    print(e)