import numpy as np

def add_column(df, column_name, values):
    if column_name in df.column_names:
        raise ValueError(f"Column {column_name} already exists.")
    if len(values) != df.data.shape[0]:
        raise ValueError("Length of values does not match number of rows.")
    
    new_data = np.column_stack((df.data, values))
    new_column_names = df.column_names + [column_name]
    return new_data, new_column_names

def remove_column(df, column_name):
    if column_name not in df.column_names:
        raise ValueError(f"Column {column_name} does not exists.")
    
    idx = df.column_names.index(column_name)
    new_data = np.delete(df.data, idx, axis=1)  # Remove the column
    new_column_names = df.column_names[:idx] + df.column_names[idx + 1:]
    return new_data, new_column_names

def sort(df, columns, ascending):
    # Validate input
    missing_columns = [col for col in columns if col not in df.column_names]
    if missing_columns:
        raise ValueError(f"Some columns do not exist in the DataFrame: {', '.join(missing_columns)}")
    
    # Get indices of columns to sort by
    column_indices = [df.column_names.index(col) for col in columns]

    sort_order = np.lexsort([df.data[:, i] for i in reversed(column_indices)])

    if not all(ascending):
        for _, asc in enumerate(reversed(ascending)):
            if not asc:
                sort_order = sort_order[::-1]
    
    df.data = df.data[sort_order]

    return df

def groupby(df, columns, first=True):
    # Validate input
    missing_columns = [col for col in columns if col not in df.column_names]
    if missing_columns:
        raise ValueError(f"Some columns do not exist in the DataFrame: {', '.join(missing_columns)}")
    
    # Get indices of columns to group by
    column_indices = [df.column_names.index(col) for col in columns]
    
    # Group data using a dictionary
    grouped = {}
    for row in df.data:
        key = tuple(row[idx] for idx in column_indices)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(row)
    
    # Select first or last entry in each group
    grouped_data = []
    for key, rows in grouped.items():
        if first:
            grouped_data.append(rows[0])
        else:
            grouped_data.append(rows[-1])
    
    # Update the DataFrame in place
    df.data = np.array(grouped_data, dtype=object)
    return df
