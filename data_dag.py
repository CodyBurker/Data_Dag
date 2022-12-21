import pandas as pd

class df2:
    def __init__(self,df=None):
        self.df = df
    
    # def __init__(self):
    #     self.df = None

    def read_csv(self, file):
        self.df = pd.read_csv(file)
        return self

    def filter(self, condition):
        self.df = self.df[condition]
        return self

    def select(self, *args):
        args = list(args)
        self.df = self.df[args]
        return self

    def groupby(self, *args):
        self.df = self.df.groupby(list(args))
        return self

    def summarize(self, *kwargs):
        self.df = self.df.aggregate(*kwargs)
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