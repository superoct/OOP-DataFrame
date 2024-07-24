def info(df):
    # Display column names
    print(f"Column Names: {df.column_names}")
    
    # Display data types of each column
    print("Data Types:")
    for i, column_name in enumerate(df.column_names):
        dtype = df.data[:, i].dtype
        print(f"  {column_name}: {dtype}")
    
    # Display number of rows
    print(f"Number of Rows: {df.data.shape[0]}")
    
    # Calculate and display memory usage
    print(f"Memory Usage: {df.data.nbytes} bytes")

def get_column(df, column_name):
    if column_name not in df.column_names:
        raise ValueError(f"Column '{column_name}' does not exist in the DataFrame.")
    idx = df.column_names.index(column_name)
    return df.data[:, idx]
