import unittest
import pandas as pd
import os

from data_dag import *

# x = pd.read_csv('test.csv')

# x = df2(pd.read_csv('test.csv'))

print(
    df2().
    read_csv('addresses.csv')
    # select('hello','world').
    # groupby('hello').
    # summarize(['sum'])
)

# print(x)

# print(
#     pd.read_csv('test.csv') + \
#         df2()
# )