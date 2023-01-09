import unittest
import pandas as pd
import os
import data_dag as dd

if __name__ == "__main__":
    test_df = pd.read_csv("mtcars.txt")
    test_dd = dd.Df2(test_df, lazy=True)
    print(test_df.head())
    test_dd.filter("mpg > 20").\
        select("mpg","cyl").\
        groupby("cyl").\
        summarize(avg_mpg="mpg:mean", max_mpg="mpg:max")
    print(test_dd.collect())

    print(
        test_dd.filter("mpg > 20").\
        collect()
    )