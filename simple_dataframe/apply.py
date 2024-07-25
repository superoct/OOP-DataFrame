import numpy as np

def apply_to_column(df, column, func):
    if column not in df.column_names:
        raise ValueError(f"Column '{column}' does not exists in the DataFrame.")
    
    col_idx = df.column_names.index(column)
    df.data[:, col_idx] = np.array([func(val) for val in df.data[:, col_idx]], dtype=object)

    return df
