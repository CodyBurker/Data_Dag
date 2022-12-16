[![data_dag](https://github.com/CodyBurker/Data_Dag/actions/workflows/python-package.yml/badge.svg)](https://github.com/CodyBurker/Data_Dag/actions/workflows/python-package.yml)

# Data Dag

Accelerate EDA and data wrangling with intelligent caching, parallel processing, and lazy evaluation while writing a reproduceable data pipeline.

## Motivation

Often times during EDA and data wrangling, we iteratively apply a few operations on a dataset. This can result in multiple copies of the dataset across various variables. When using a notebook, it can be easy to accidentally edit a line out of order, leaving a notebook that cannot be run top to bottom after a kernel restart without errors.

Also, familiar pandas operations can be slow when applied to large datasets. This can be mitigated by using parallel processing, but this can be difficult to implement in a notebook.

Lastly, if an earlier step in an anaysis needs to change (e.g. including a new column), it can be difficult to know which steps need to be rerun. 

Python DAG libraries like [Airflow](https://airflow.apache.org/) and [Luigi](https://luigi.readthedocs.io/en/stable/) can help with this, but they can be difficult to use for EDA and data wrangling due to their complexity.

This aims to be a lightweight, flexible tool to support quick exploration by providing:
- *Intelligent caching*: Save steps after they run once, and only rerun steps that depend on the changed step
- *Lazy evaluation*: Only run steps when they are needed
- *Dot Notation*: Add steps to a pipeline using dot notation (instead of a dictionary)
- *Parallel processing*: Run steps in parallel when possible
- *`dplyr` like syntax*: Use intuitive verbs like `select` and `filter` to apply operations to a dataset
- *Data pipeline visualization*: Visualize how data is transformed from one step to the next
- *Data profiling*: Profile data to see how it changes from one step to the next (e.g. statistics, missing values, etc.)


### Example
Defining a pipeline is as simple as defining a dictionary of steps. Each step is a function that can take in one (or more) datasets as a parameter and exports them. The steps can be defined in any order, and the pipeline will automatically figure out the dependencies between steps.

```python
```python
from data_dag import lazy, dep, pipeline, lazy_read_csv
ex_pipe = pipeline()

ex_pipe.define({
        # Read in a csv file
        "first": lazy_read_csv(
            params = {'filepath_or_buffer': "test.csv"}),
        # Rename a column
        "second": lazy(
            func = lambda x,y: x.rename(**y), 
            params = {'x': dep('first'), 'y': {'columns': {'old_name': 'new_name'}}})
    })
```
Accessing a step will run the step and all of dependencies (and nothing more!). The result of the step is cached, so subsequent calls to the step will return the cached result.

```python
ex_pipe.get_step('second') # This will read in a file, rename a column, and return the dataframe
ex_pipe.get_step('second') # This will return the cached result
```
Changes to a step will cause all steps that depend on it to be rerun when they are accessed.

For example, if we redfine the `first` step to read in a different file, both steps will be rerun when they are accessed. This works if redefining the pipeline later in the code, or if rerunning the cells in a notebook.

```python
ex_pipe.define({
        # Read in a different file
        "first": lazy_read_csv(
            params = {'filepath_or_buffer': "test_2.csv"}),
        # Rename a column
        "second": lazy(
            func = lambda x,y: x.rename(**y), 
            params = {'x': dep('first'), 'y': {'columns': {'old_name': 'new_name'}}})
    })
ex_pipe.get_step('second') # This will read in the new file, rename a column, and return the dataframe while caching the new result
```