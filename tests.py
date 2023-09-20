import sys
sys.path.append('../DATADAG')
import unittest
from interpreter import Interpreter

# Test read_csv
class TestReadCSV(unittest.TestCase):
    def test_read_csv(self):
        interpreter = Interpreter()
        interpreter.run('read_csv "data/iris.csv"')
        self.assertEqual(interpreter.current_df.collect().shape, (150, 5))

# Test select
class TestSelect(unittest.TestCase):
    def test_select(self):
        interpreter = Interpreter()
        interpreter.run("""
                        read_csv "data/iris.csv"
                        select {sepal.length} {sepal.width}
                        """)
        # Check that only the selected columns are in the dataframe
        all_columns = interpreter.current_df.collect().columns
        self.assertEqual(len(all_columns), 2)

# Test filter
class TestFilter(unittest.TestCase):
    def test_filter(self):
        interpreter = Interpreter()
        interpreter.run("""
                        read_csv "data/iris.csv"
                        filter {sepal.length} > 5
                        """)
        filtered_df = interpreter.current_df.collect()
        self.assertEqual(filtered_df.shape, (118, 5))

# Test show
# TODO

# Test add_column
class TestAddColumn(unittest.TestCase):
    def test_add_column(self):
        interpreter = Interpreter()
        interpreter.run("""
                        read_csv "data/iris.csv"
                        add_column {sepal.sum} = {sepal.length} + {sepal.width}
                        """)
        all_columns = interpreter.current_df.collect().columns
        self.assertIn('sepal.sum', all_columns)
        # Check the output of the very first row to ensure the calculation is correct
        result_df = interpreter.current_df.collect()
        first_row = result_df['sepal.sum'][0]
        self.assertEqual(first_row, 8.6)

# Test group by and summarize
class TestGroupBySummarize(unittest.TestCase):
    def test_group_by_summarize(self):
        interpreter = Interpreter()
        interpreter.run("""
                        read_csv "data/iris.csv"
                        group_by {variety}
                        summarize {sepal.length} = mean({sepal.length})
                        """)
        result_df = interpreter.current_df.collect()
        self.assertEqual(result_df.shape, (3, 2))


if __name__ == '__main__':
    unittest.main()