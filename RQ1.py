import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path

dataset_root = "./Dataset_Modified"
output_parquet_path = "./RQ1.parquet"

all_passenger_sums = []
all_passenger_avgs = []

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print("Processing: " + parquet_file.name)

    df = pq.read_table(parquet_file).to_pandas()

    df['month'] = df['Trip_Start'].dt.strftime('%Y-%m')
    table = pa.Table.from_pandas(df, preserve_index=False)

    passenger_sum = table.group_by(['Vendor_ID', 'month']).aggregate([('Passengers', 'sum')]).to_pandas()
    passenger_avg = table.group_by(['Vendor_ID', 'month']).aggregate([('Passengers', 'mean')]).to_pandas()

    all_passenger_sums.append(passenger_sum)
    all_passenger_avgs.append(passenger_avg)

passenger_sum = pd.concat(all_passenger_sums).groupby(['Vendor_ID', 'month']).sum().reset_index()
passenger_avg = pd.concat(all_passenger_avgs).groupby(['Vendor_ID', 'month']).mean().reset_index()

merged_data = pd.merge(
    passenger_sum,
    passenger_avg,
    on=['Vendor_ID', 'month'],
    suffixes=('_sum', '_mean')
)

merged_data['Ride_Count'] = merged_data['Passengers_sum'] / merged_data['Passengers_mean']

try:
    merged_data.to_parquet(output_parquet_path, index=False)
except Exception as e:
    print(e)