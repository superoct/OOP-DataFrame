from simple_dataframe import MyDataFrame
from datetime import datetime, timedelta
import random
import string
from io import StringIO
import numpy as np

# Example Usage
pk_columns = ['id', 'name']
sort_columns = ['sv_op_timestamp']

random_range = 5 #10  # Adjusted for simplicity

data = {
    'id': [1, 2, 3, 4], #[random.randint(0, 999999) for _ in range(random_range)],
    'name': ['Alice', 'Bob', 'Charlie', 'David'],#[''.join(random.choices(string.ascii_letters, k=random.randint(1, 24))) for _ in range(random_range)],
    'amount': [100.5, 200.75, 300.25, 400.0],#[round(random.uniform(0, 99.9), 2) for _ in range(random_range)],
    'sv_op_timestamp': [datetime(2020, 1, 1) + timedelta(days=random.randint(0, (datetime.now() - datetime(2020, 1, 1)).days), seconds=random.randint(0, 24 * 60 * 60 - 1)) for _ in range(random_range)]
}
df = MyDataFrame(data)
print("Original Data:\n", df.get_data())
print()

df.info()
print()

# Adding a column
new_pk = [p + '_etl_pk' for p in pk_columns + sort_columns]

for _, (pk_c, pk_n) in enumerate(zip(pk_columns + sort_columns, new_pk)):
    df.add_column(pk_n, [str(pk).lower() for pk in df.get_column(pk_c)])
print("After Adding Column:\n")
df.info()

# Sorts the data
df.sort(new_pk, [True, True, False])
print("After Sorting:\n", df.get_data())
print()

# Group by
df.groupby(new_pk, first=True)
print("After Group By:\n", df.get_data())
print()

for col in df.column_names:
    if col.endswith('_etl_pk'):
        df.remove_column(col)

# Slicing the data
#sliced_df = df.slice([(0, 2), (4, 5)], keep=False, view=False)
#print("Sliced Data (Copy):\n", sliced_df.get_data())
#print()
#print("After Slicing Original DataFrame:\n", df.get_data())
#print()

print("Merge DataFrame")
data = {
    'id': [3, 4, 5, 6], #[random.randint(0, 999999) for _ in range(random_range)],
    'name': ['Charlie', 'David', 'Eve', 'Frank'],#[''.join(random.choices(string.ascii_letters, k=random.randint(1, 24))) for _ in range(random_range)],
    'amount': [350.0, 450.5, 150.25, 250.75],
    'sv_op_timestamp': [datetime(2020, 1, 1) + timedelta(days=random.randint(0, (datetime.now() - datetime(2020, 1, 1)).days), seconds=random.randint(0, 24 * 60 * 60 - 1)) for _ in range(random_range)]
}

df2 = MyDataFrame(data)

merge_df = df.merge(df2, on=pk_columns, how='inner', suffix=('_etl_hive', '_etl_sql'), indicator=True)

print(merge_df.get_data())
print()

merge_df.rename_column(old='_etl_hive', new='')

merge_df.info()

csv_data = """id,name,amount
1,Alice ,100.5
2,NULL,200.75
3,Charlie,NULL
"""

csv_data = csv_data.replace(',', chr(1))

print(csv_data)
print()

# Use StringIO to simulate a file object
file_like = StringIO(csv_data)

# Test reading a CSV file from StringIO
df = MyDataFrame.read_csv(file_like, delimiter=chr(1), na_values=['NULL'], dtype=str, fillna='ACTUAL_NULL')

# Display the DataFrame information and data
df.info()
print(df.data)
print()

df.apply_to_column('name', lambda x: x.lower().strip())

df.info()
print(df.data)

"""# Merge DataFrames
merged_df = df1.merge(df2, on='id', how='inner')
print("Merged DataFrame (Inner Join):")
merged_df.info()
print(merged_df.get_data())

merged_df = df1.merge(df2, on='id', how='outer', suffix=('_left', '_right'))
print("Merged DataFrame (Outer Join):")
merged_df.info()
print(merged_df.get_data())

merged_df = df1.merge(df2, on='id', how='left', indicator=True)
print("Merged DataFrame (Left Join with Indicator):")
merged_df.info()
print(merged_df.get_data())

merged_df = df1.merge(df2, on='id', how='right', indicator='_source')
print("Merged DataFrame (Right Join with Custom Indicator):")
merged_df.info()
print(merged_df.get_data())

Inner - Acts correctly, brings data matching between both df
Outer - Acts correctly, brings data not matching between both
Left  - Acts correctly, brings left only values
Right - Acts correctly, brings right only values
"""
