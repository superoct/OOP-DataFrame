import numpy as np

def merge(df1, df2, on, how='inner', suffix=('_df1', '_df2'), indicator=False, MyDataFrame=None):
    if isinstance(on, str):
        on = [on]

    # Ensure the columns exist in both dataframes
    missing_columns_df1 = [col for col in on if col not in df1.column_names]
    missing_columns_df2 = [col for col in on if col not in df2.column_names]

    if missing_columns_df1 or missing_columns_df2:
        raise ValueError(f"Some columns do not exist in the DataFrame: {', '.join(missing_columns_df1 + missing_columns_df2)}")

    # Get indices of columns to join on
    join_indices_df1 = [df1.column_names.index(col) for col in on]
    join_indices_df2 = [df2.column_names.index(col) for col in on]

    # Create a dictionary to map rows based on join keys
    join_dict = {}
    for row in df2.data:
        key = tuple(row[idx] for idx in join_indices_df2)
        if key not in join_dict:
            join_dict[key] = []
        join_dict[key].append(row)

    # List to hold the merged rows
    merged_data = []
    merge_column = []

    for row in df1.data:
        key = tuple(row[idx] for idx in join_indices_df1)
        if key in join_dict and how == 'inner':
            for matching_row in join_dict[key]:
                merged_data.append(np.concatenate([row, [matching_row[idx] for idx, col in enumerate(df2.column_names) if col not in on]]))
                merge_column.append("both")
        elif key not in join_dict and how == 'left': #key not in join_dict and how in ('left', 'outer'):
            null_row = [None] * (len(df2.column_names) - len(on))
            merged_data.append(np.concatenate([row, null_row]))
            merge_column.append("left_only")

    """
    if how in ('right', 'outer'):
        for row in df2.data:
            key = tuple(row[idx] for idx in join_indices_df2)
            if key not in {tuple(row[idx] for idx in join_indices_df1) for row in df1.data}:
                null_row = [None] * (len(df1.column_names) - len(on))
                merged_data.append(np.concatenate([null_row, row]))
                merge_column.append("right_only")
    """

    # Create new column names
    new_column_names = []
    for col in df1.column_names:
        if col in on or col not in df2.column_names:
            new_column_names.append(col)
        else:
            new_column_names.append(col + suffix[0])

    for col in df2.column_names:
        if col not in on and col not in df1.column_names:
            new_column_names.append(col)
        elif col not in on:
            new_column_names.append(col + suffix[1])

    # Add the merge indicator column if requested
    if indicator:
        if isinstance(indicator, bool):
            indicator = '_merge'
        merged_data = [row.tolist() + [merge_col] for row, merge_col in zip(merged_data, merge_column)]
        new_column_names.append(indicator)

    # Return new merged DataFrame
    return MyDataFrame({col: [row[idx] for row in merged_data] for idx, col in enumerate(new_column_names)})
