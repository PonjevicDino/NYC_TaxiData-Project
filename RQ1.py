import os
import pandas as pd
import numpy as np
from pathlib import Path

import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

dataset_root = "./Dataset_Modified/2015"

dataset = pq.ParquetDataset(dataset_root + "/")
dataset_table = dataset.read()

years = pc.year(dataset_table.column("Trip_Start"))
years = pc.cast(years, pa.string())

months = pc.month(dataset_table.column("Trip_Start"))
months = pc.cast(months, pa.string())
months = pc.ascii_lpad(months, 2, '0')

hyphens = pa.array(['-'] * len(dataset_table))

years = years.combine_chunks() if isinstance(years, pa.ChunkedArray) else years
hyphens = hyphens.combine_chunks() if isinstance(hyphens, pa.ChunkedArray) else hyphens
months = months.combine_chunks() if isinstance(months, pa.ChunkedArray) else months

year_series = years.to_pandas()
hyphen_series = hyphens.to_pandas()
padded_month_series = months.to_pandas()

dates = year_series + hyphen_series + padded_month_series
dates = pa.Array.from_pandas(dates)

new_dataset_table = dataset_table.append_column("month", dates)

passenger_sum = new_dataset_table.group_by(['Vendor_ID', 'month']).aggregate([('Passengers', 'sum')]).to_pandas().sort_values(by=['Vendor_ID', 'Passengers_sum'], ascending=False)
print(passenger_sum)

passenger_avg = new_dataset_table.group_by(['Vendor_ID', 'month']).aggregate([('Passengers', 'mean')]).to_pandas().sort_values(by=['Vendor_ID', 'Passengers_mean'], ascending=False)
print(passenger_avg)

merged_data = pd.merge(
    passenger_sum,
    passenger_avg,
    on=['Vendor_ID', 'month'],
    suffixes=('_sum', '_mean')
)
merged_data['Ride_Count'] = merged_data['Passengers_sum'] / merged_data['Passengers_mean']
print(merged_data)

output_parquet_path = "./RQ1.parquet"
try:
    merged_data.to_parquet(output_parquet_path, index=False)
except Exception as e:
    print(e)