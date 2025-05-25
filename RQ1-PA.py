import os
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path

dataset_root = "C:/Dataset_Modified/"
output_parquet_path = "./RQ1-PA.parquet"

all_passenger_sums = []
all_passenger_avgs = []

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print(f"Processing: {parquet_file.name}")

    table = pq.read_table(parquet_file)
    trip_start_int = pc.cast(table['Trip_Start'], pa.int64())
    trip_start_naive = pc.cast(trip_start_int, pa.timestamp('us', tz=None))
    table = table.set_column(
        table.schema.get_field_index('Trip_Start'),
        'Trip_Start',
        trip_start_naive
    )

    year = pc.year(table['Trip_Start'])
    month_num = pc.month(table['Trip_Start'])

    padded_month_num = pc.add(month_num, 100)
    month_str = pc.utf8_slice_codeunits(
        pc.cast(padded_month_num, pa.string()),
        start=1,
        stop=3
    )

    year_str = pc.cast(year, pa.string())
    month = pc.binary_join_element_wise(year_str, month_str, '-')

    table = table.append_column('month', month)

    sum_table = table.group_by(['Vendor_ID', 'month']).aggregate([('Passengers', 'sum')])
    avg_table = table.group_by(['Vendor_ID', 'month']).aggregate([('Passengers', 'mean')])

    all_passenger_sums.append(sum_table)
    all_passenger_avgs.append(avg_table)


combined_sum_aggregated = pa.concat_tables(all_passenger_sums).group_by(['Vendor_ID', 'month']).aggregate(
    [('Passengers_sum', 'sum')]
)
combined_sum = combined_sum_aggregated.rename_columns(['Vendor_ID', 'month', 'Passengers_sum'])

combined_avg_aggregated = pa.concat_tables(all_passenger_avgs).group_by(['Vendor_ID', 'month']).aggregate(
    [('Passengers_mean', 'mean')]
)
combined_avg = combined_avg_aggregated.rename_columns(['Vendor_ID', 'month', 'Passengers_mean'])


merged_table = combined_sum.join(combined_avg, keys=['Vendor_ID', 'month'])
merged_table = merged_table.append_column('Ride_Count',
                                          pc.divide(merged_table['Passengers_sum'], merged_table['Passengers_mean']))

try:
    pq.write_table(merged_table, output_parquet_path)
except Exception as e:
    print(e)