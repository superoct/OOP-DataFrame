from concurrent.futures import ThreadPoolExecutor
import numpy as np
import warnings


class MyDataFrame:
    def __init__(self, data):
        self.column_names = list(data.keys())
        self.data = np.array([list(row) for row in zip(*data.values())], dtype=object)
    
    def __len__(self):
        return self.data.shape[0]
    
    def get_data(self):
        return self.column_names, self.data.tolist()
    
    def info(self):
        print("Column Info:")
        for i, column_name in enumerate(self.column_names):
            dtype = self.data[:, i].dtype
            print(f" {column_name}: {dtype}")
        
        print(f"Number of Rows: {self.data.shape[0]}")

        print(f"Memory Usage: {self.data.nbytes} bytes")
    
    def get_column(self, column_name):
        if column_name in self.column_names:
            index = self.column_names.index(column_name)
            
            return self.data[:, index]
        else:
            raise ValueError(f"Column '{column_name}' does not exists in the DataFrame.")

    def _apply_function(self, func, *args, **kwargs):
        return func(self.data,self.column_names, *args, **kwargs)

    def apply(self, func, *args, **kwargs):
        with ThreadPoolExecutor() as executor:
            future = executor.submit(self._apply_function, func, *args, **kwargs)
            return future.result()

    def add_column(self, column_name, values):
        def add_column_func(data, column_names, column_name, values):
            new_data = np.copy(data)  # Make a copy of the data
            new_column = np.array(values).reshape(-1, 1)
            new_data = np.hstack((new_data, new_column))  # Add the new column
            new_column_names = column_names + [column_name]
            return new_data, new_column_names
        
        self.data, self.column_names = self.apply(add_column_func, column_name, values)
        return self
    
    def sort_values(self, column_list, ascending):
        if len(column_list) != len(ascending):
            raise ValueError(f"The length of the columns and ascending list must be the same. Length of columns: {len(column_list)} not equals to Length of ascending: {len(ascending)}")
        
        missing_columns = [col for col in column_list if col not in self.column_names]
        if missing_columns:
            raise ValueError(f"Some columns do not exists in the DataFrame: {', '.join(missing_columns)}")
        
        column_indices = [self.column_names.index(col) for col in column_list]

        sort_order = np.array(ascending)
        sort_order = np.where(sort_order, 1, -1)

        sort_keys = tuple(self.data[:, idx] for idx in reversed(column_indices))

        sorted_indices = np.lexsort(sort_keys)
        self.data = self.data[sorted_indices]

        for idx, order in zip(reversed(column_indices), sort_order):
            if order == -1:
                self.data = np.flip(self.data, axis=0)
        
        return self
    
    def groupby(self, column_list, first=True):
        missing_columns = [col for col in column_list if col not in self.column_names]
        if missing_columns:
            raise ValueError(f"Some columns do not exists in the DataFrame: {', '.join(missing_columns)}")
        
        column_indices = [self.column_names.index(col) for col in column_list]

        grouped = {}
        for row in self.data:
            key = tuple(row[idx] for idx in column_indices)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(row)
        
        grouped_data = []
        for key, rows in grouped.items():
            if first:
                grouped_data.append(rows[0])
            else:
                grouped_data.append(rows[-1])
        
        self.data = np.array(grouped_data, dtype=object)
        return self

    def remove_column(self, column_name):
        def remove_column_func(data, column_names, column_name):
            if column_name in column_names:
                index = column_names.index(column_name)
                new_data = np.delete(data, index, axis=1)  # Remove the column
                new_column_names = column_names[:index] + column_names[index + 1:]
                return new_data, new_column_names
            return data, column_names
        
        self.data, self.column_names = self.apply(remove_column_func, column_name)
        return self
    
    def slice(self, ranges, keep=False, view=False):
        mask = np.zeros(len(self.data), dtype=bool)
        for start, end in ranges:
            mask[start:end] = True
        
        if keep:
            if view:
                return MyDataFrame({col: self.data[mask, idx] for idx, col in enumerate(self.column_names)})
            else:
                return MyDataFrame({col: np.copy(self.data[mask, idx]) for idx, col in enumerate(self.column_names)})
        else:
            if view:
                warnings.warn("View cannot be created when keep is False. Returning a copy instead.")
            slice_data = MyDataFrame({col: np.copy(self.data[mask, idx]) for idx, col in enumerate(self.column_names)})
            self.data = self.data[~mask]
            return slice_data