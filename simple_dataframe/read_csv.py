import numpy as np
import csv
from io import StringIO

def read_csv(file, delimiter=',', na_values=None, dtype=None, fillna=None):
    data = []
    column_names = []

    if isinstance(file, str):
        file = open(file, 'r')
    elif not hasattr(file, 'read'):
        raise ValueError("The file parameter should be a file path or file-like object.")
    
    with file:
        reader = csv.reader(file, delimiter=delimiter)
        column_names = next(reader)

        for row in reader:
            if na_values:
                row = [fillna if cell in na_values else cell for cell in row]
            data.append(row)
    
    data = np.array(data, dtype=object)

    if dtype:
        if isinstance(dtype, dict):
            for col_name, col_type in dtype.items():
                if col_name in column_names:
                    col_idx = column_names.index(col_name)

                    try:
                        data[:, col_idx] = data[:, col_idx].astype(col_type)
                    except ValueError:
                        raise ValueError(f"Cannot convert column '{col_name}' to type {col_type}")
        else:
            try:
                data = data.astype(dtype)
            except ValueError:
                raise ValueError(f"Cannot convert data to type {dtype}")
    
    return column_names, data
