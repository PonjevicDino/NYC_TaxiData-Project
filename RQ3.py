import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

dataset_root = "./Dataset_Modified"
output_file = './RQ3.parquet'

all_counts = []
all_total_counts = []

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print("Processing: " + parquet_file.name)

    df = pq.read_table(parquet_file).to_pandas()

    file_counts = df.groupby(['Type', 'Payment_Type']).size().reset_index(name='Count')
    all_counts.append(file_counts)

    file_totals = df.groupby('Type').size().reset_index(name='Total_Count')
    all_total_counts.append(file_totals)


combined_counts = pd.concat(all_counts, ignore_index=True)
final_counts = combined_counts.groupby(['Type', 'Payment_Type'])['Count'].sum().reset_index()

combined_totals = pd.concat(all_total_counts, ignore_index=True)
final_totals = combined_totals.groupby('Type')['Total_Count'].sum().reset_index()

merged = pd.merge(final_counts, final_totals, on='Type')
merged['Percentage'] = (merged['Count'] / merged['Total_Count']) * 100
result = merged[['Type', 'Payment_Type', 'Percentage']]

try:
    pq.write_table(pa.Table.from_pandas(result), output_file)
except Exception as e:
    print(e)