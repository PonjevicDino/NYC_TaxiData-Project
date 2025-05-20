from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from collections import defaultdict

dataset_root = "./Dataset_Modified"
output_file = "./RQ5.parquet"

sum_tip_dist = defaultdict(float)
count_tip_dist = defaultdict(int)
sum_tip_dur = defaultdict(float)
count_tip_dur = defaultdict(int)

results = []

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print("Processing: " + parquet_file.name)

    df = pq.read_table(parquet_file).to_pandas()

    df['Trip_Distance'] = df['Trip_Distance'].replace(0, np.nan)
    df['Trip_Duration'] = df['Trip_Duration'].replace(0, np.nan)
    df['Tip_Per_Distance'] = df['Tip_Amount'] / df['Trip_Distance']
    df['Tip_Per_Duration'] = df['Tip_Amount'] / df['Trip_Duration']

    for ride_type, group in df.groupby('Type'):
        valid_distances = group['Tip_Per_Distance'].dropna()
        sum_tip_dist[ride_type] += valid_distances.sum()
        count_tip_dist[ride_type] += len(valid_distances)

        valid_durations = group['Tip_Per_Duration'].dropna()
        sum_tip_dur[ride_type] += valid_durations.sum()
        count_tip_dur[ride_type] += len(valid_durations)

for ride_type in set(sum_tip_dist.keys()).union(set(sum_tip_dur.keys())):
    try:
        factor_mile = sum_tip_dist[ride_type] / count_tip_dist[ride_type]
    except (KeyError, ZeroDivisionError):
        factor_mile = np.nan

    try:
        factor_min = (sum_tip_dur[ride_type] / count_tip_dur[ride_type]) * 60
    except (KeyError, ZeroDivisionError):
        factor_min = np.nan

    results.append({
        'Type': ride_type,
        'Factor_Tip_Mile': factor_mile,
        'Factor_Tip_Minute': factor_min
    })

result_df = pd.DataFrame(results)

try:
    pq.write_table(pa.Table.from_pandas(result_df), output_file)
except Exception as e:
    print(e)