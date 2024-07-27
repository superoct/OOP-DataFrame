def rename_column(df, old, new:str):
    if isinstance(old, str):
        old = [old]
    
    df.column_names = [str(col).replace(str(old_col), str(new)) for col in df.column_names for old_col in old]
    
    return df
