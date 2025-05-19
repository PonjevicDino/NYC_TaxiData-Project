import os
import pandas as pd
import numpy as np
from pathlib import Path

import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

# Load the Dataset

dataset_root = "./Dataset_Modified/2015"

dataset = pq.ParquetDataset(dataset_root + "/")
dataset_table = dataset.read()
##print(dataset_table)

# 2015-01 | Vendor1: ~3Ps. on 40 Rides | Vendor ...
months = pc.month(dataset_table.column("Trip_Start"))
new_dataset_table = dataset_table.append_column("month", months)
passenger_sum = new_dataset_table.group_by(['Vendor_ID', 'month']).aggregate([('Passengers', 'sum')]).to_pandas().sort_values(by=['Vendor_ID', 'Passengers_sum'], ascending=False)
print(passenger_sum)
passenger_avg = new_dataset_table.group_by(['Vendor_ID', 'month']).aggregate([('Passengers', 'mean')]).to_pandas().sort_values(by=['Vendor_ID', 'Passengers_mean'], ascending=False)
print (passenger_avg)