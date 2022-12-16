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
        return self.pipes[step].eval()

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

        # Check the values are either strings or dep
        x = [x for x in params.values() if isinstance(x, str) or isinstance(x, dep)]
        if len(x) != len(params.values()):
            raise Exception('All parameter values must be strings or deps')

        return(None)
    
    # Function to compare this lazy to another to determine if the parameters have changed
    def is_changed(self, other):
        return None


    
    def hash():
        # Hash self and previous funtions recursively?
        # Might be a better way to do this.
        return(0)
    
    def eval(self):
        # If cache is valid, return stored input
        # Else Recursively evaluate inputs
        # Then evalute self.
        if not self.tainted and self.results is not None:
            return self.results
        else:
            # Evaluate any dep params
            new_params = {}
            for param in iter(self.params):
                if isinstance(self.params[param], dep):
                    new_params[param] = self.params[param].eval()
                else:
                    new_params[param] = self.params[param]
            # Cache results
            self.results = self.func(**self.params)
            print("Evaluate!")
    
import hashlib
# Or are results maybe cached here?
class dep:
    def __init__(self, prev_node: str):
        self.prev_node = prev_node
    def eval(self):
        return 0
    def __str__(self) -> str:
        return("dep")

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
    def __init__(self, filepath, params: dict) -> None:
        self.hash = None
        self.filepath = filepath
        self.cache = None
        self.params = params
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
            self.cache = pd.read_csv(self.filepath, **self.params)
        return self.cache