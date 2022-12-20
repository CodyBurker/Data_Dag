import pandas as pd

class df2:
    def __init__(self,df=None):
        self.df = df
    
    # def __init__(self):
    #     self.df = None

    def filter(self, condition):
        self.df = self.df[condition]
        return self

    def select(self, *args):
        args = list(args)
        self.df = self.df[args]
        return self

    def groupby(self, *args):
        self.df = self.df.groupby(args)
        return self

    def summarize(self, **kwargs):
        self.df = self.df.aggregate(**kwargs)
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
        return str(self.df)
    
    def __radd__(self, other):
        print('RADD')
        if isinstance(other, pd.DataFrame):
            print('input:')
            # print(other)
            self.df = other
