import unittest
import pandas as pd
import os

from data_dag import *

# x = pd.read_csv('test.csv')

# x = df2(pd.read_csv('test.csv'))

x = pd.read_csv('test.csv') + \
    df2()

print(x)

print(
    pd.read_csv('test.csv') + \
        df2()
)