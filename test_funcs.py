import unittest
import pandas as pd
import os

from data_dag import *

# x = pd.read_csv('test.csv')

# x = df2(pd.read_csv('test.csv'))

# print(
#     pd.read_csv('overseas-trade-indexes-September-2022-quarter-provisional-csv.csv', low_memory=False)
# )

# x = (df2().
#     read_csv('mtcars.txt').
#     select('cyl','mpg','gear').
#     groupby('cyl', 'gear').
#     summarize('mean').
#     filter('mpg<20')
# )
# print(x)

print(df2().
    read_csv('mtcars.txt').
    select('mpg').
    filter('mpg < 20').
    summarize('count')
)


# print(
#     df2().
#     read_csv('mtcars.txt').
#     select('mpg').
#     collect().
#     query('mpg < 20')
# )



# print(x)

# print(
#     pd.read_csv('test.csv') + \
#         df2()
# )