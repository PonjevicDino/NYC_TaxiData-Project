import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path

dataset_root = "C:/Dataset_Modified"
output_file = './RQ3-PA.parquet'

all_counts_tables = []
all_total_counts_tables = []

EMPTY_COUNTS_SCHEMA = pa.schema([
    ('Type', pa.string()),
    ('Payment_Type', pa.int64()),
    ('Count', pa.int64())
])

EMPTY_TOTALS_SCHEMA = pa.schema([
    ('Type', pa.string()),
    ('Total_Count', pa.int64())
])

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print(f"Processing: {parquet_file.name}")

    table = pq.read_table(parquet_file)

    try:
        payment_type_idx = table.schema.get_field_index('Payment_Type')
        if table.schema.field('Payment_Type').type != pa.int64():
            table = table.set_column(payment_type_idx, 'Payment_Type', pc.cast(table['Payment_Type'], pa.int64()))
    except Exception as e:
        print(e)
        continue

    file_counts_temp = table.group_by(['Type', 'Payment_Type']).aggregate([
        ('Type', 'count')
    ])
    file_counts_temp = file_counts_temp.rename_columns([
        'Type', 'Payment_Type', 'Count'
    ])
    all_counts_tables.append(file_counts_temp)


    file_totals_temp = table.group_by('Type').aggregate([
        ('Type', 'count')
    ])
    file_totals_temp = file_totals_temp.rename_columns([
        'Type', 'Total_Count'
    ])
    all_total_counts_tables.append(file_totals_temp)

if all_counts_tables:
    combined_counts = pa.concat_tables(all_counts_tables)
else:
    combined_counts = pa.Table.from_pylist([], schema=EMPTY_COUNTS_SCHEMA)

if combined_counts.num_rows > 0:
    final_counts_aggregated = combined_counts.group_by(['Type', 'Payment_Type']).aggregate([
        ('Count', 'sum')
    ])
    final_counts = final_counts_aggregated.rename_columns([
        'Type', 'Payment_Type', 'Count'
    ])
else:
    final_counts = pa.Table.from_pylist([], schema=EMPTY_COUNTS_SCHEMA)

if all_total_counts_tables:
    combined_totals = pa.concat_tables(all_total_counts_tables)
else:
    combined_totals = pa.Table.from_pylist([], schema=EMPTY_TOTALS_SCHEMA)

if combined_totals.num_rows > 0:
    final_totals_aggregated = combined_totals.group_by('Type').aggregate([
        ('Total_Count', 'sum')
    ])
    final_totals = final_totals_aggregated.rename_columns([
        'Type', 'Total_Count'
    ])
else:
    final_totals = pa.Table.from_pylist([], schema=EMPTY_TOTALS_SCHEMA)

if final_counts.num_rows > 0 and final_totals.num_rows > 0:
    merged = final_counts.join(final_totals, keys=['Type'], join_type='inner')
else:
    merged_schema = pa.schema([
        final_counts.schema.field('Type'),
        final_counts.schema.field('Payment_Type'),
        final_counts.schema.field('Count'),
        final_totals.schema.field('Total_Count') if 'Total_Count' in final_totals.schema.names else pa.field(
            'Total_Count', pa.int64()),
        pa.field('Percentage', pa.float64())
    ])
    merged = pa.Table.from_pylist([], schema=merged_schema)

if merged.num_rows > 0 and 'Count' in merged.schema.names and 'Total_Count' in merged.schema.names:
    count_col_float = pc.cast(merged['Count'], pa.float64())
    total_count_col_float = pc.cast(merged['Total_Count'], pa.float64())

    ratio = pc.if_else(pc.equal(total_count_col_float, 0.0),
                       pa.scalar(None, type=pa.float64()),
                       pc.divide(count_col_float, total_count_col_float))

    percentage_col = pc.multiply(ratio, pa.scalar(100.0, type=pa.float64()))

    if 'Percentage' in merged.schema.names:
        merged = merged.drop_columns(['Percentage']).append_column('Percentage', percentage_col)
    else:
        merged = merged.append_column('Percentage', percentage_col)
elif 'Percentage' not in merged.schema.names:
    merged = merged.append_column('Percentage', pa.array([None] * merged.num_rows, type=pa.float64()))

if 'Type' in merged.schema.names and 'Payment_Type' in merged.schema.names and 'Percentage' in merged.schema.names:
    result = merged.select(['Type', 'Payment_Type', 'Percentage'])
else:
    result_schema = pa.schema([
        pa.field('Type', pa.string()),
        pa.field('Payment_Type', pa.int64()),
        pa.field('Percentage', pa.float64())
    ])
    result = pa.Table.from_pylist([], schema=result_schema)

try:
    pq.write_table(result, output_file)
except Exception as e:
    print(e)