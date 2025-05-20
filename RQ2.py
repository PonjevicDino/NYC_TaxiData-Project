import os
import pandas as pd
import numpy as np
from pathlib import Path

import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

dataset_root = "./Dataset_Modified"

dataset = pq.ParquetDataset(dataset_root + "/")
dataset_table = dataset.read().to_pandas()

grouped_data = dataset_table.groupby(['Start_Zone', 'End_Zone']).agg(
    Count=('Trip_Distance', 'size'),
    Avg_Distance=('Trip_Distance', 'mean')
).reset_index()

grouped_data = grouped_data.loc[grouped_data.groupby('Start_Zone')['Count'].idxmax()]

grouped_data = grouped_data[['Start_Zone', 'End_Zone', 'Avg_Distance']].copy()
grouped_data = grouped_data.rename(columns={'End_Zone': 'Favorite_End_Zone'})

avg_total_distance_per_start_zone = dataset_table.groupby('Start_Zone')['Trip_Distance'].mean().reset_index()
avg_total_distance_per_start_zone = avg_total_distance_per_start_zone.rename(columns={'Trip_Distance': 'Avg_Total_Distance'})

final_result_df = pd.merge(grouped_data, avg_total_distance_per_start_zone,
                           on='Start_Zone',
                           how='left')

final_result_df = final_result_df[['Start_Zone', 'Favorite_End_Zone', 'Avg_Distance', 'Avg_Total_Distance']]
output_pa_table = pa.Table.from_pandas(final_result_df, preserve_index=False)

output_file_name = './RQ2.parquet'
try:
    pq.write_table(output_pa_table, output_file_name)
except Exception as e:
    print(e)
