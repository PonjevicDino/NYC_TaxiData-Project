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

counts = dataset_table.groupby(['Type', 'Payment_Type']).size().reset_index(name='Count')
total_counts = counts.groupby('Type')['Count'].sum().reset_index(name='Total_Count')
merged = pd.merge(counts, total_counts, on='Type')

merged['Percentage'] = (merged['Count'] / merged['Total_Count']) * 100

result = merged[['Type', 'Payment_Type', 'Percentage']]

try:
    result.to_parquet('./RQ3.parquet')
except Exception as e:
    print(e)