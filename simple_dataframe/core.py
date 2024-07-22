import numpy as np
from concurrent.futures import ThreadPoolExecutor
import warnings
from .view import info, get_column
from .insert import add_column, remove_column, sort, groupby, slice_data

class MyDataFrame:
    def __init__(self, data):
        self.column_names = list(data.keys())
        self.data = np.array([list(row) for row in zip(*data.values())], dtype=object)
    
    def __len__(self):
        return self.data.shape[0]
    
    def get_data(self):
        return self.data.tolist(), self.column_names
    
    def info(self):
        info(self)

    def get_column(self, column_name):
        return get_column(self, column_name)

    def _apply_function(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)

    def apply(self, func, *args, **kwargs):
        with ThreadPoolExecutor() as executor:
            future = executor.submit(self._apply_function, func, *args, **kwargs)
            return future.result()

    def add_column(self, column_name, values):
        self.data, self.column_names = self.apply(add_column, column_name, values)
        return self
    
    def sort(self, columns, ascending):
        sort(self, columns, ascending)
        return self
    
    def groupby(self, columns, first=True):
        groupby(self, columns, first)
        return self

    def remove_column(self, column_name):
        self.data, self.column_names = self.apply(remove_column, column_name)
        return self

    def slice(self, ranges, keep=False, view=False):
        return slice_data(self, ranges, keep, view, MyDataFrame)
