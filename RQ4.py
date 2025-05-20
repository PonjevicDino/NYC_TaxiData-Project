from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

dataset_root = "./Dataset_Modified"
output_file = "./RQ4.parquet"

total_sums = {}
total_counts = {}

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print("Processing: " + parquet_file.name)

    df = pq.read_table(parquet_file).to_pandas()
    file_group = df.groupby('End_Zone')['Forward_Stored_Flag'].agg(['sum', 'count'])

    for end_zone, row in file_group.iterrows():
        if end_zone in total_sums:
            total_sums[end_zone] += row['sum']
            total_counts[end_zone] += row['count']
        else:
            total_sums[end_zone] = row['sum']
            total_counts[end_zone] = row['count']


result = pd.DataFrame({
    'End_Zone': list(total_sums.keys()),
    'sum': list(total_sums.values()),
    'count': list(total_counts.values())
})

result['Finished Rides (online)'] = 100 - (result['sum'] / result['count']) * 100
result = result[['End_Zone', 'Finished Rides (online)']].sort_values('Finished Rides (online)', ascending=False)

try:
    pq.write_table(pa.Table.from_pandas(result), output_file)
except Exception as e:
    print(e)