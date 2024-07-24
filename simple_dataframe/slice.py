import numpy as np
import warnings

def slice(df, ranges, keep=False, view=False, MyDataFrame=None):
    # Create a boolean mask for the rows to keep
    mask = np.zeros(len(df.data), dtype=bool)

    for start, end in ranges:
        mask[start:end] = True
    
    if keep:
        if view:
            # Return a view of the selected data
            return MyDataFrame({col: df.data[mask, idx] for idx, col in enumerate(df.column_names)}, view=True)
        else:
            # Return a copy of the selected data
            return MyDataFrame({col: np.copy(df.data[mask, idx]) for idx, col in enumerate(df.column_names)})
    else:
        if view:
            warnings.warn("View cannot be created when keep is False. Returning a copy instead.")
        # Return a copy of the selected data and delete those rows from the DataFrame
        sliced_data = MyDataFrame({col: np.copy(df.data[mask, idx]) for idx, col in enumerate(df.column_names)})
        df.data = df.data[~mask]
        return sliced_data

def slice_batch(df, batch_size):
    total_rows = df.data.shape[0]

    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)

        yield df.slice([(start, end)], keep=True, view=True)
