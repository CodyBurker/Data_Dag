import unittest
import pandas as pd
import os
import pytest

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
    
    def test_lazy_read_csv(self):
        f = pd.read_csv("test.csv")
        x = lazy_read_csv({"filepath_or_buffer": "test.csv"})

        self.assertTrue(not x.cache)
        self.assertTrue(not x.hash)

        self.assertEqual(f.iloc[0,0], x.eval().iloc[0,0])

        self.assertTrue(isinstance(x.cache, pd.DataFrame))
        self.assertTrue(isinstance(x.hash, str))
        # Check that dataframes are equal
        self.assertTrue(f.equals(x.cache))
        


    def test_lazy_read_csv_in_pipeline(self):
        ex_pipe = pipeline()

        ex_pipe.define({
            "first": lazy_read_csv(
                params = {'filepath_or_buffer': "test.csv"})
        })
        # Check if the hash and cache are empty
        self.assertTrue(not ex_pipe.pipes['first'].cache)
        self.assertTrue(not ex_pipe.pipes['first'].hash)

        # Check if the step is a dataframe
        self.assertTrue(isinstance(ex_pipe.get_step('first'), pd.DataFrame))
        
        # Check if the cache is a dataframe
        self.assertTrue(isinstance(ex_pipe.pipes['first'].cache, pd.DataFrame))

        # Check if the hash is a string
        self.assertTrue(isinstance(ex_pipe.pipes['first'].hash, str))
    
    def test_lazy_read_csv_cache_invalidation(self):
        # Create new minimal CSV file to test cache invalidation
        with open('test2.csv', 'w') as f:
            f.write('a,b,c\n1,2,3\n4,5,6\n7,8,9')
        test_pipeline = pipeline()
        test_pipeline.define({
            "first": lazy_read_csv(
                params = {'filepath_or_buffer': "test2.csv"})
        })
        # Check if the hash and cache are empty
        self.assertTrue(not test_pipeline.pipes['first'].cache)
        self.assertTrue(not test_pipeline.pipes['first'].hash)
        # Save the cache to comparae later
        cache = test_pipeline.pipes['first'].cache
        # Check if the step is a dataframe
        self.assertTrue(isinstance(test_pipeline.get_step('first'), pd.DataFrame))
        # Check if the cache is non-empty
        self.assertTrue(test_pipeline.pipes['first'].hash)
        # Change the CSV file
        with open('test2.csv', 'w') as f:
            f.write('a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12')
        # Read the step again
        test_pipeline.get_step('first')
        # Check if the cache is different
        self.assertTrue(not test_pipeline.pipes['first'].cache.equals(cache))
        # Delete the test file
        os.remove('test2.csv')
    
    def test_lazy_lambda_eval_in_pipeline(self):
        ex_pipe = pipeline()
        ex_pipe.define({
            "first": lazy_read_csv(
                params = {'filepath_or_buffer': "test.csv"}),
            "second": lazy(
                func = lambda x,y: x.rename(**y), 
                params = {'x': dep('first'), 'y': {'columns': {'hello': 'what'}}})
        })
        self.assertTrue(isinstance(ex_pipe.get_step('second'), pd.DataFrame))
        self.assertTrue(ex_pipe.get_step('second').columns[0] == 'what')

if __name__ == "__main__":
    
    # # Example code
    # ex_pipe = pipeline()
    # ex_pipe.define({
    #     "first": lazy_read_csv(
    #         params = {'filepath_or_buffer': "test.csv"}),

    #     "second": lazy(
    #         func = lambda x,y: x.rename(**y), 
    #         params = {'x': dep('first'), 'y': {'columns': {'hello': 'what'}}})
    # })
    
    unittest.main()
    