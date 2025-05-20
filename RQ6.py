from pathlib import Path

import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

import numpy as np

import pandas as pd

dataset_root = "./Dataset_Modified/2019"

dataset = pq.ParquetDataset(dataset_root + "/")
dataset_table = dataset.read().to_pandas()

dataset_table['Request_Time'] = pd.to_datetime(dataset_table['Request_Time'])
dataset_table['OnScene_Time'] = pd.to_datetime(dataset_table['OnScene_Time'])

fhvhv_filter = dataset_table[dataset_table['Type'] == 'FHVHV'].copy()
fhvhv_filter['Response_Time'] = (fhvhv_filter['OnScene_Time'] - fhvhv_filter['Request_Time']).dt.total_seconds()

average_response_times = fhvhv_filter.groupby('Vendor_ID')['Response_Time'].mean().reset_index()
average_response_times.rename(columns={'Response_Time': 'Average_Response_Time'}, inplace=True)

print(average_response_times)

try:
    average_response_times.to_parquet("./RQ6.parquet", index=False)
except Exception as e:
    print(e)