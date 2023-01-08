import unittest
import pandas as pd
import os

<<<<<<< HEAD
from data_dag import lazy, dep, pipeline, lazy_read_csv, lazy_step
=======
from data_dag import *
>>>>>>> d27938c0d1ef17e44082bdb87d817fbd9bc43f8b

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
    select('mpg', 'gear','cyl').
    groupby('gear', 'cyl').
    summarize(avg='mpg:mean')

)


# print(
#     df2().
#     read_csv('mtcars.txt').
#     select('mpg').
#     collect().
#     query('mpg < 20')
# )



# print(x)

<<<<<<< HEAD
if __name__ == "__main__":
    # unittest.main()
    pipetest = pipeline()

    pipetest.add_step(
        lazy_step('hello', print, {'hello': 'world'})
    )

    pipetest >\
        lazy_step('hello2', print, {'hello': 'world2'})

    print(pipetest.pipes)
    
=======
# print(
#     pd.read_csv('test.csv') + \
#         df2()
# )
>>>>>>> d27938c0d1ef17e44082bdb87d817fbd9bc43f8b
