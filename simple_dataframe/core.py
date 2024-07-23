import numpy as np
from concurrent.futures import ThreadPoolExecutor
import warnings
from .view import info, get_column
from .insert import add_column, remove_column, sort, groupby, slice_data
from .merge import merge
from .rename_column import rename_column

class MyDataFrame:
    """ Initialization 
        Reads the data from a dictionary that contains a list for values. The keys will be interpret as the column_names and the value will be interpret as the rows.

        The column names will be stored in a list.
        The data will stored in a 2-dimensional array using numpy.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)
    """
    def __init__(self, data):
        self.column_names = list(data.keys())
        self.data = np.array([list(row) for row in zip(*data.values())], dtype=object)
    

    """ Len
        Returns the amount of rows the MyDataFrame variable contains.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)

        row_count = len(df)

        prit(row_count)
    """
    def __len__(self):
        return self.data.shape[0]
    

    """ Empty
        Returns a bool if the data is empty or not.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)

        df.empty --False will be return
    """
    @property
    def empty(self):
        return self.data.size == 0
    
    
    """ Get Data
        It prints the list of column names and then the data.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)

        print(df.get_data())
            ['id', 'amount'], [['00', 0.00], ['01', 0.10], ['02', 0.20], ['03', 0.30]]
    """
    def get_data(self):
        return self.column_names, self.data.tolist()
    

    """ Info
        Returns some status information about the data frame. Column name(s), data type, length of the data and bytes of usage of the data.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)

        df.info()
            Column names: ['id', 'amount']
            Data Types:
                id: object
                amount: object
            Number of Rows: 4
            Memory Usage: 64 bytes
    """
    def info(self):
        info(self)


    """ Get Column
        Returns the data from a column.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)

        print(df.get_column('id'))
            ['00' '01' '02' '03']
    """
    def get_column(self, column_name):
        return get_column(self, column_name)


    """ 
    
    """
    def _apply_function(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)

    """ 

    """
    def apply(self, func, *args, **kwargs):
        with ThreadPoolExecutor() as executor:
            future = executor.submit(self._apply_function, func, *args, **kwargs)
            return future.result()


    """ Add Column
        Adds a column to the data frame.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)

        df.add_column('name', ['a', 'b', 'c', 'd'])
    """
    def add_column(self, column_name, values):
        self.data, self.column_names = self.apply(add_column, column_name, values)
        return self
    

    """ Sort
        Sorts the data based on the columns provided and the ascending order provided.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)

        df.sort('id', False)
    """
    def sort(self, columns, ascending):
        sort(self, columns, ascending)
        return self
    

    """ GroupBy
        Groups the data based on the columns provided and wheter to grab the first results or the last results.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)

        df.groupby('id', first=False)
    """
    def groupby(self, columns, first=True):
        groupby(self, columns, first)
        return self


    """ Remove Column
        Removes the column from the data frame.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)

        df.remove_column('amount')
    """
    def remove_column(self, column_name):
        self.data, self.column_names = self.apply(remove_column, column_name)
        return self


    """ Slice
        Creates either a view or a new data frame based on the configuration. 
        If keep and view are False, then a copy is created. A new data frame will be created and remove the data from the original data frame.
        If keep is False and view is True, then a warning message will appear that a copy will be made instead of a view.
        If keep is True and view is False, then a copy is created. A new data frame will be created and remove the data from the original data frame.
        If keep is True and view is True, then a view is created. Changes to the data frame should reflect on the view, but the view can't make any changes to the data frame.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)

        df_view = df.slice([0,1], keep=True, view=True)
        df_copy = df.slice([0,1], keep=False, view=False) #This might cause the df_view to be empty.
    """
    def slice(self, ranges, keep=False, view=False):
        return slice_data(self, ranges, keep, view, MyDataFrame)
    

    """ Merge
        Merge the data between two data frames, itself and another data frame.

        other: MyDataFrame
            Is another data frame to be compared with.
        
        on: str|list
            Accepts one or multiple columns to be merge on. 
        
        how: str
            It will tell the merge on how to do the merge, 'inner' or 'left'.
        
        suffix: tuple
            It will be added to the columns matching between the data frames. [0] is for the self data frame. [1] is for the other data frame.
        
        indicator: bool
            An indicator column will be added to the data frame, Will contain the values: 'both' and 'left_only'

        data1 = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        data2 = {
            "id": ['01', '02', '03', '04'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df1 = MyDataFrame(data1)

        df1 = MyDataFrame(data2)

        df_merge = df1.merge(df2, on='id', how='inner', suffix=('left', 'right'), indicator=True)
    """
    def merge(self, other, on, how='inner', suffix=('_df1', '_df2'), indicator=False):
        return merge(self, other, on, how, suffix, indicator, MyDataFrame)
    

    """ Rename Column
        Renames the provided column from the old name to the new name.

        data = {
            "id": ['00', '01', '02', '03'],
            "amount": [0.00, 0.10, 0.20, 0.30]
        }

        df = MyDataFrame(data)

        df.rename_column(old='id', new='id_num')
    """
    def rename_column(self, old='', new=''):
        rename_column(self, old, new)
        return self
