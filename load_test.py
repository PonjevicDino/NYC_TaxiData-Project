import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

dataSet_path = "../Datasets/NYC_TaxiData"

# Try to read and print the whole Dataset (loads ~7ß GB into RAM) --> Fails - malloc!
dataset = pq.ParquetDataset(dataSet_path + "/")
table_partitioned = dataset.read()
print(table_partitioned)