from pathlib import Path

import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

import numpy as np

dataset_root = "./Dataset_Modified"

dataset = pq.ParquetDataset(dataset_root + "/")
dataset_table = dataset.read().to_pandas()

dataset_table['Trip_Distance'] = dataset_table['Trip_Distance'].replace(0, np.nan)
dataset_table['Trip_Duration'] = dataset_table['Trip_Duration'].replace(0, np.nan)

dataset_table['Tip_Per_Distance'] = dataset_table['Tip_Amount'] / dataset_table['Trip_Distance']
dataset_table['Tip_Per_Duration'] = dataset_table['Tip_Amount'] / dataset_table['Trip_Duration']

dataset_table = dataset_table.groupby('Type').agg(
    Factor_Tip_Mile=('Tip_Per_Distance', 'mean'),
    Factor_Tip_Minute=('Tip_Per_Duration', lambda x: x.mean() * 60)
).reset_index()

print(dataset_table)

try:
    dataset_table.to_parquet("./RQ5.parquet", index=False)
except Exception as e:
    print(e)