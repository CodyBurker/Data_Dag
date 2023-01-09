import pandas as pd

# Todo
## Rewrite read_csv

class Df2:
    """
    TODO: Document
    """
    def __init__(self,df=None, lazy = False):
        self.df = df
        self.collected_df = None
        self.groupby_columns = []
        self.lazy = lazy
        # List of lambda functions to apply if lazy
        self.operations = []

    def read_csv(self, *args, **kwargs):
        if self.lazy:
            self.operations.append(("read_csv",args,kwargs))
            return self
        else:
            self.df = pd.read_csv(*args, **kwargs)
            return self
    # Functions to apply to the dataframe
    def __filter__(self, condition):
        return lambda df: df.query(condition)
    
    def __select__(self, *args):
        return lambda df: df[list(args)]
    
    def __groupby__(self, *args):
        return lambda df: df.groupby(list(args))
    
        
    def __summarize__(self,**kwargs):
        def _summarize(df):
            """
            Dplyr summarize function
            Given a kwargs dictionary, of the form {new_col_name: "col_name:func_name"}
            apply the functions to the columns and return a new dataframe with the
            results.
            """

            # Parse Kwargs into a dictionary of {col_name: func_name}
            aggregations = {}
            for name, func in kwargs.items():
                col, func_name = func.split(":")
                aggregations[col] = func_name
            
            df = df.aggregate(aggregations).reset_index()

            col_names = {
                x : x for x in df.columns[:-len(aggregations):] # Get original column names
            }

            col_names.update({
                x:y for x,y in zip(df.columns,kwargs.keys()) # Get aggregate names: passed kwargs
            })
            df.rename(col_names)
            return df
        return _summarize

    
    # Methods to show user
    # Depending on lazy, either apply the operation or add it to the list of operations

    def filter(self, condition):
        if self.lazy:
            self.operations.append(self.__filter__(condition))
            return self
        else:
            self.df = self.__filter__(condition)(self.df)
            return self
    
    def select(self, *args):
        if self.lazy:
            self.operations.append(self.__select__(*args))
            return self
        else:
            self.df = self.__select__(*args)(self.df)
            return self
    
    def groupby(self, *args):
        if self.lazy:
            self.operations.append(self.__groupby__(*args))
            return self
        else:
            self.df = self.__groupby__(*args)(self.df)
            return self
    
    def summarize(self, **kwargs):
        if self.lazy:
            self.operations.append(self.__summarize__(**kwargs))
            return self
        else:
            self.df = self.__summarize__(**kwargs)(self.df)
            return self
    
    def collect(self):
        if self.lazy:
            for operation in self.operations:
                self.collected_df = operation(self.df)
            return self.collected_df
        else:
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
