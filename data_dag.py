import pandas as pd

class df2:
    def __init__(self,df=None):
        self.df = df
        self.groupby_columns = []
    
    # def __init__(self):
    #     self.df = None

    def read_csv(self, *args, **kwargs):
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

        self.df = self.df.rename(columns={col: name for name, func in kwargs.items() for col in self.groupby_columns})
        return self
    
    def collect(self):
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