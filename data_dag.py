# Might have to rethink this and move more logic to the pipeline class
# E.g. results can't be in the lazy class because it gets reinitialized.

import pandas as pd

class pipeline:

    def __init__(self):
        self.pipes = {}
        return None
    
    def define(self, pipes: dict):

        # Check that pipes is actually a dict
        if not isinstance(pipes, dict):
            raise Exception('Pipeline must be a dictionary')

        # Create a graph and sort it topologically
        # get count of number of dependencies in graph
        self.pipes = pipes
        

    def preview(self):
        # Evaluate (if needed) and output all leaf/terminal nodes as dictionary
        return 0
    
    def get_step(self, step:str):
        if not isinstance(step, str):
            raise Exception('Step must be a string')
        # Check if step is in the pipeline
        if step not in self.pipes.keys():
            raise Exception(f'Step {step} not in pipeline')
        # Check if step has valid cache
        if self.pipes[step].tainted:
            # For each parameter in the step, if it is a dep, then get it's value
            # Otherwise, just use the value
            params = {}
            for k,v in self.pipes[step].params.items():
                if isinstance(v, dep):
                    params[k] = self.get_step(v.step())
                else:
                    params[k] = v
            # If there is a func and it is a lambda, then evaluate it
            if hasattr(self.pipes[step], 'func') and isinstance(self.pipes[step].func, type(lambda:0)):
                # Evaluate the function with the parameters
                self.pipes[step].results = self.pipes[step].func(**params)
            # Otherwise, assume it has an eval function and use that with the parameters
            else:
                self.pipes[step].results = self.pipes[step].eval()
        return(self.pipes[step].results)    


# Lazy Evaluation Node
class lazy:
    tainted = True # Define whether results are still valid
    dependencies = []
    results = None # Hold evaluation

    def __init__(self, func, params: dict):
        self.func = func
        self.params = params
        self.tainted = True

        # Check the function is a function
        if not callable(func):
            raise Exception('Function must be a function')
        
        if not isinstance(params, dict):
            raise Exception('Params must be passed as a dictionary')

        # Check the keys of the params are all strings
        x = [x for x in params.keys() if isinstance(x, str)]
        if len(x) != len(params.keys()):
            raise Exception('All parameter names must be strings')

        # # Check the values are either strings or dep
        # x = [x for x in params.values() if isinstance(x, str) or isinstance(x, dep)]
        # if len(x) != len(params.values()):
        #     raise Exception('All parameter values must be strings or deps')

        return(None)
    
    # Function to compare this lazy to another to determine if the parameters have changed
    def is_changed(self, other):
        return None


    
    def hash():
        # Hash self and previous funtions recursively?
        # Might be a better way to do this.
        return(0)
    
import hashlib
# Or are results maybe cached here?
class dep:
    def __init__(self, prev_node: str):
        self.prev_node = prev_node
    def eval(self):
        return 0
    def __str__(self) -> str:
        return("dep")
    def step(self) -> str:
        return(self.prev_node)

# Helper graph function
# From 
#https://favtutor.com/blogs/topological-sort-python#:~:text=Topological%20sort%20is%20an%20algorithm,not%20contain%20any%20directed%20cycle.
from collections import defaultdict

class Graph:
    def __init__(self,n):
        self.graph = defaultdict(list)
        self.N = n

    def addEdge(self,m,n):
        self.graph[m].append(n)

    def sortUtil(self,n,visited,stack):
        visited[n] = True
        for element in self.graph[n]:
            if visited[element] == False:
                self.sortUtil(element,visited,stack)
        stack.insert(0,n)

    def topologicalSort(self):
        visited = [False]*self.N
        stack =[]
        for element in range(self.N):
            if visited[element] == False:
                self.sortUtil(element,visited,stack)
        print(stack)
    
class lazy_read_csv:
    # This is a method, init guvnah?
    def __init__(self, params: dict) -> None:
        self.hash = None
        self.params = params
        self.cache = None
        self.tainted = True
        self.filepath = params['filepath_or_buffer']
        pass

    # Read in the dataframe
    def eval(self):
        # Check hash to see if file has changed
        md5 = hashlib.md5()
        with open(self.filepath, 'rb') as file:
            while True:
                BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
                data = file.read(BUF_SIZE)
                if not data:
                    break;
                md5.update(data)
        new_hash = md5.hexdigest()
        # If the file hasn't changed, return the cache
        if self.hash == new_hash:
            return self.cache
        else:
            self.hash = new_hash
            self.cache = pd.read_csv(**self.params)
        # self.cache = pd.read_csv(**self.params)
        # self.cache = pd.read_csv('test.csv')
        return self.cache