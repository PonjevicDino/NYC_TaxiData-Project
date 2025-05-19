from pathlib import Path

import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

dataset_root = "./Dataset_Modified/2015"

dataset = pq.ParquetDataset(dataset_root + "/")
dataset_table = dataset.read().to_pandas()

online_rides_table = 100 - (dataset_table.groupby('End_Zone')['Forward_Stored_Flag'].mean() * 100)

online_rides_table = online_rides_table.reset_index()
online_rides_table.columns = ['End_Zone', 'Finished Rides (online)']

online_rides_table = online_rides_table.sort_values(by='Finished Rides (online)', ascending=False)

print(online_rides_table)

try:
    online_rides_table.to_parquet("./RQ4.parquet", index=False)
except Exception as e:
    print(e)
