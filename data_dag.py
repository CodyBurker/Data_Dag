import pandas as pd

class Df2:
    """
    TODO: Document
    """
    def __init__(self,df=None, lazy = False):
        self.df = df
        self.groupby_columns = []
        self.lazy = lazy
        self.operations = []
        
    
    # def __init__(self):
    #     self.df = None

    def read_csv(self, *args, **kwargs):
        if self.lazy:
            self.operations.append(("read_csv",args,kwargs))
            return self
        else:
            self.df = pd.read_csv(*args, **kwargs)
            return self

    def filter(self, condition):
        self.df = self.df.query(condition)
        return self
    # def filter(self, condition):
    #     def filter_func(row):
    #         return eval(condition)
    #     self.df = self.df[self.df.apply(filter_func, axis=1)]
    #     return self

    def select(self, *args):
        args = list(args)
        self.df = self.df[args]
        return self

    def groupby(self, *args):
        # self.df = self.df.groupby(list(args))
        self.groupby_columns = list(args)
        return self

    def summarize(self, **kwargs):
        if self.groupby_columns:
            self.df = self.df.groupby(self.groupby_columns)
        aggregations = {}
        for name, func in kwargs.items():
            col, func_name = func.split(":")
            aggregations[col] = func_name
        
        self.df = self.df.aggregate(aggregations).reset_index()

        col_names = {
            x : x for x in self.df.columns[:-len(aggregations):] # Get original column names
        }

        col_names.update({
            x:y for x,y in zip(self.df.columns,kwargs.keys()) # Get aggregate names: passed kwargs
        })

        self.df.rename(col_names)

        return self
    
    def collect(self):
        # Apply operations
        return self.df

    def __add__(self, other):
        if isinstance(other, DplyrDataFrame):
            self.df = other.df
        else:
            raise TypeError("Can only concatenate DplyrDataFrame with another DplyrDataFrame")
        return self
    
    def __str__(self):
        if self.df is None:
            return("DataFrame is not initialized")
        return str(self.df)
    
    def __radd__(self, other):
        if isinstance(other, pd.DataFrame):
            self.df = other
        else:
            raise TypeError(f"Can only concatenate DplyrDataFrame with a dataframe, not {type(other)}")
