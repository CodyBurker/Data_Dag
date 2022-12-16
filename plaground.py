import unittest
import pandas as pd

from data_dag import lazy, dep, pipeline, lazy_read_csv

class TestLazy(unittest.TestCase):

    def test_lazy_function(self):
        l = lazy(print, {'hello':'world'})
        self.assertDictEqual(l.params, {'hello': 'world'})

    def test_lazy_function_except(self):
        with self.assertRaises(Exception) as context:
            l = lazy('hi', {'hello':'world'})

    def test_lazy_function_param(self):
        with self.assertRaises(Exception) as context:
            l = lazy(print, 'hi')

    def test_lazy_correct(self):
        l = lazy(print, {'hello': 'world'}) # Should work
        self.assertDictEqual(l.params, {'hello': 'world'})

    def test_lazy_error_key(self):
        with self.assertRaises(Exception) as context:
            lazy(print, {5: 'world'}) # Throws an exception

class TestPipeline(unittest.TestCase):
    def test_pipeline_is_dict(self):
        with self.assertRaises(Exception):
            x = pipeline()
            x.define('x')

class TestLazyReadCSV(unittest.TestCase):
    
    def test_lazy_read(self):
        f = pd.read_csv("test.csv")
        x = lazy_read_csv("test.csv", {})

        self.assertTrue(not x.cache)
        self.assertTrue(not x.hash)

        self.assertEqual(f.iloc[0,0], x.eval().iloc[0,0])

        self.assertTrue(isinstance(x.cache, pd.DataFrame))
        self.assertTrue(isinstance(x.hash, str))

        self.assertEqual(f.iloc[1,0], x.eval().iloc[1,0])

if __name__ == "__main__":
    
    # Example code
    ex_pipe = pipeline()
    ex_pipe.define({
        "first": lazy_read_csv(
            filepath = "test.csv", 
            params = {}),

        "second": lazy(
            func = lambda x: x.rename({'hello':'hiya'}), 
            params = {'x': dep('first')})
    })

    print(ex_pipe.pipes)
    print(ex_pipe.pipes['first'].eval())
    print(ex_pipe.pipes['second'].func)
    print(ex_pipe.pipes['second'].eval())
    # unittest.main()
    