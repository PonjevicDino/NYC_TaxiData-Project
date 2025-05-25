from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

dataset_root = "C:/Dataset_Modified"
output_file = "./RQ6-PA.parquet"

all_aggregations_tables = []

SCHEMA_AGG_FILE = pa.schema([
    ('Vendor_ID', pa.string()),
    ('RT_S_sum_file', pa.float64()),
    ('RT_S_count_file', pa.int64())
])

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print(f"Processing: {parquet_file.name}")
    if "fhvhv" not in parquet_file.name.lower():
        print(f"Skipping {parquet_file.name}.")
        continue

    table = pq.read_table(parquet_file)
    if pa.types.is_dictionary(table['Type'].type):
        table = table.filter(pc.equal(table['Type'], pa.scalar('FHVHV', type=table['Type'].type)))
    elif not pa.types.is_string(table['Type'].type):
        table = table.filter(pc.equal(pc.cast(table['Type'], pa.string()), pa.scalar('FHVHV')))
    else:
         table = table.filter(pc.equal(table['Type'], pa.scalar('FHVHV')))


    req_time_ts = pc.cast(table['Request_Time'], pa.timestamp('us', tz='UTC')) if 'Request_Time' in table.column_names else None
    onscene_time_ts = pc.cast(table['OnScene_Time'], pa.timestamp('us', tz='UTC')) if 'OnScene_Time' in table.column_names else None
    trip_start_ts = pc.cast(table['Trip_Start'], pa.timestamp('us', tz='UTC')) if 'Trip_Start' in table.column_names else None

    if onscene_time_ts is not None and trip_start_ts is not None:
        onscene_time_filled = pc.if_else(pc.is_null(onscene_time_ts), trip_start_ts, onscene_time_ts)
    elif onscene_time_ts is not None:
        onscene_time_filled = onscene_time_ts
    elif trip_start_ts is not None:
        onscene_time_filled = trip_start_ts

    response_time_duration = pc.subtract(onscene_time_filled, req_time_ts)

    response_time_microseconds_int = pc.cast(response_time_duration, pa.int64())
    response_time_microseconds_float = pc.cast(response_time_microseconds_int, pa.float64())
    response_time_seconds_float = pc.divide(response_time_microseconds_float, pa.scalar(1000000.0, type=pa.float64()))

    current_table_processed = pa.Table.from_arrays([
        table['Vendor_ID'],
        response_time_seconds_float
    ], names=['Vendor_ID', 'Response_Time_S'])

    current_table_processed = current_table_processed.filter(
        pc.is_valid(current_table_processed['Response_Time_S'])
    )
    current_table_processed = current_table_processed.filter(
        pc.greater_equal(current_table_processed['Response_Time_S'], pa.scalar(0.0, type=pa.float64()))
    )

    agg_per_file = current_table_processed.group_by('Vendor_ID').aggregate([
        ('Response_Time_S', 'sum'),
        ('Response_Time_S', 'count')
    ])
    agg_per_file = agg_per_file.rename_columns([
        'Vendor_ID', 'RT_S_sum_file', 'RT_S_count_file'
    ])
    all_aggregations_tables.append(agg_per_file)

if not all_aggregations_tables:
    combined_aggregations = pa.Table.from_pylist([], schema=SCHEMA_AGG_FILE)
else:
    combined_aggregations = pa.concat_tables(all_aggregations_tables)


if combined_aggregations.num_rows > 0:
    final_aggregations_temp = combined_aggregations.group_by('Vendor_ID').aggregate([
        ('RT_S_sum_file', 'sum'),
        ('RT_S_count_file', 'sum')
    ])
    final_aggregations = final_aggregations_temp.rename_columns([
        'Vendor_ID', 'Total_Sum_Response_Time', 'Total_Count_Response_Time'
    ])

    final_aggregations = final_aggregations.filter(
        pc.greater(final_aggregations['Total_Count_Response_Time'],
                   pa.scalar(0, type=final_aggregations['Total_Count_Response_Time'].type)) # Match type
    )

    if final_aggregations.num_rows > 0:
        sum_rt_col = final_aggregations['Total_Sum_Response_Time']
        count_rt_col = final_aggregations['Total_Count_Response_Time']

        avg_response_time_col = pc.divide(
            pc.cast(sum_rt_col, pa.float64()),
            pc.cast(count_rt_col, pa.float64())
        )

        final_aggregations = final_aggregations.append_column('Average_Response_Time', avg_response_time_col)
        result_table = final_aggregations.select(['Vendor_ID', 'Average_Response_Time'])
    else:
        result_table = pa.Table.from_pylist([], schema=pa.schema([
            ('Vendor_ID', SCHEMA_AGG_FILE.field('Vendor_ID').type),
            ('Average_Response_Time', pa.float64())
        ]))
else:
    result_table = pa.Table.from_pylist([], schema=pa.schema([
        ('Vendor_ID', SCHEMA_AGG_FILE.field('Vendor_ID').type),
        ('Average_Response_Time', pa.float64())
    ]))

try:
    pq.write_table(result_table, output_file)
except Exception as e:
    print(e)
