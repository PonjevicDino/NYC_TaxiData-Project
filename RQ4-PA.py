from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

dataset_root = "C:/Dataset_Modified"
output_file = "./RQ4-PA.parquet"

all_aggregations_tables = []

SCHEMA_FOR_EMPTY_AGG = pa.schema([
    ('End_Zone', pa.string()),
    ('Forward_Stored_Flag_sum', pa.int64()),
    ('Forward_Stored_Flag_count', pa.int64())
])

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print(f"Processing: {parquet_file.name}")

    table = pq.read_table(parquet_file)

    temp_agg = table.group_by('End_Zone').aggregate([
        ('Forward_Stored_Flag', 'sum'),
        ('Forward_Stored_Flag', 'count')
    ])
    all_aggregations_tables.append(temp_agg)

if not all_aggregations_tables:
    combined_table = pa.Table.from_pylist([], schema=SCHEMA_FOR_EMPTY_AGG)
else:
    combined_table = pa.concat_tables(all_aggregations_tables)

if combined_table.num_rows > 0:
    final_agg_temp = combined_table.group_by('End_Zone').aggregate([
        ('Forward_Stored_Flag_sum', 'sum'),
        ('Forward_Stored_Flag_count', 'sum')
    ])

    final_agg = final_agg_temp.rename_columns([
        'End_Zone',
        'total_sf_flag_sum',
        'total_sf_flag_count'
    ])
else:
    final_agg_schema = pa.schema([
        ('End_Zone', SCHEMA_FOR_EMPTY_AGG.field('End_Zone').type),
        ('total_sf_flag_sum', SCHEMA_FOR_EMPTY_AGG.field('Forward_Stored_Flag_sum').type),
        ('total_sf_flag_count', SCHEMA_FOR_EMPTY_AGG.field('Forward_Stored_Flag_count').type)
    ])
    final_agg = pa.Table.from_pylist([], schema=final_agg_schema)

if final_agg.num_rows > 0:
    sum_col_float = pc.cast(final_agg['total_sf_flag_sum'], pa.float64())
    count_col_float = pc.cast(final_agg['total_sf_flag_count'], pa.float64())

    ratio_sf = pc.divide(sum_col_float, count_col_float)
    percentage_sf = pc.multiply(ratio_sf, pa.scalar(100.0, type=pa.float64()))

    finished_rides_online_col = pc.subtract(pa.scalar(100.0, type=pa.float64()), percentage_sf)
    final_agg_with_percentage = final_agg.append_column('Finished Rides (online)', finished_rides_online_col)
else:
    final_agg_with_percentage_schema = final_agg.schema.append(pa.field('Finished Rides (online)', pa.float64()))
    final_agg_with_percentage = pa.Table.from_pylist([], schema=final_agg_with_percentage_schema)


result_selected = final_agg_with_percentage.select(['End_Zone', 'Finished Rides (online)'])
result_sorted = result_selected.sort_by([('Finished Rides (online)', 'descending')])

try:
    pq.write_table(result_sorted, output_file)
except Exception as e:
    print(e)