Would this be easier to use as a python library rather than a whole language?


# To-do:
 - Create Parsing Expression Grammar to define langauge syntax
 - Create parser (using Pest? https://pest.rs/)
 - Create parse tree items (e.g. functions?)

# Functions
### Read in data 
* CSV
* Excel
* Previoius variables

### Write data
* CSV
* Excel
* Print to console
* Write to variable in memory (to be used for further pipelines?)

### Manipulate data
* Filter
    * Use built-in pandas features?
* Select
* Arrange
* Join

### Mutate
 * Pass in lambda function to create new file
 * Create parallelized map

### Features
* (Fast?) native dataframes
* Take advantage of multithreading easily (by default?)
* Pipelining
    - Pass data to next functions as tuples. Allow access to them via func[1] notation
    - Cache pipeline (hash function, previous steps?)
    - See output at any step
        - Previews of smaller amount of data
    - Multiple pipelines running concurrently
        - Build DAG that can be operated in parallel
* Send processing to other nodes? Or not, Spark already exists
* Minmize side effects - an entire script is run at once and idempotent (except IO?)
    - Apart from writing/reading the same file, rerunning an entire file will not change it.
# Playground
```
read file:"test.csv" skip:4 > 
    mem: my_dataframe

my_var > 
    print

rec: my_dataframe > 
    filter age>24 & age <65
    arrange age:desc
    mem: my_dataframe2

rec: my_dataframe2 > 
    print
```
```
read(file="my_file.csv", skip=4) > 
    mem(var = 1)

read(file="my_file2.csv"), mem > 
    join(left=1,right=2)
```
```
read(file="my_file1.csv") > 
    mem(var = "file1")

read(file = "my_file2.csv) > 
    mem(var = "file2")

rem(var = "file1"), rem(var = "file2") > 
    filter(1, active=True), 2 >
    $join(1,2,left="hello",right="world", type="left) > 
    mem(var = "pipe2")
```

`$` is to indicate we want a preview of that step - execute up until that point.

Algorithm
 1. Build dag
 2. Check for output (`$` or `collect`)

 ```
read(file="test.csv"), read(file="test2.csv") > 
    write[1](var = "file1"), write[2](var="file2")

read(var = "file1"), read(var = "file2") > 
    $filter[1](y==True), $filter.[2](x == 1) >
    join[1,2](type="left", left="hello", right="world") >
    write[1](file="joined.csv")
 ```
Use the `[1,2]` to indicate which of previous dataframe(s) are passed to that function.

Break down functions, params
```
function      = [$|][A-z|0-0]\\[\n\]([param]+)
params        = [A-z]=[A-z]
```

# Example Implementation in Python
```python
pipe = pipeline([
    step([read(file = "test.csv", read(file="test2.csv"))]),
    step(filter(var = 1, lambda x: x if x.height < 20])),
    prev(var = 1)
])
```
Or maybe using dictionaries?
```python
# Pass a list of steps to take
# Each step is a dictionary with the name of the output as the key, and the step as the value
pipe = pipeline([
        {
        'input1':read(file="test.csv"),
        'input2':read(file='test2.csv')
        },
        {
            'filtered1': filter(
                data = 'input1', 
                criteria = lambda x: x if x.height < 100
                )
        },
        {
            'joined': join(
                left = 'filtered1', right = 'input2',
                left_key = "pk", right_key = "fk"
            )
        },
        {
            'output': write(input = 'joined', file = 'write.csv')
        }
])

pipeline.run()
```
Everytime the pipeline is run, it will cache results. If there are already cached inputs, then use those.

Maybe use just straight up functions? e.g.

````python
pipe = pipeline({
    'first_csv'     : lazy(pd.read_csv,{'file':'hello world.csv'}),
    'second_csv'    : lazy(pd.read_csv, {'file':'second.csv'}),
    'joined'        : lazy(pd.merge, {"left": "first_csv", "right": "second_csv", "on": "key"}),
    'output'        : apply(lambda x: x.to_csv, {'x': 'joined', 'file':'joined.csv'})
})

pipeline.preview() # Default to showing latest node so that when we re/run, it uses cached previous steps.
````

The preview should show the latest/lowest (not sure how to define this, all leaf nodes?) nodes on the DAG.
Should we have input nodes that represent input files/IO? That way we can hash/cache those? so we can update if the file has changed.
This would have to be a custom object (e.g. `lazy_read`) that checks the file hash each run, and determines if it needs to re-read it. 
Then yield a file path when passed to read_csv or other function. However that would work.

````python
pipe = pipeline({
    'hello_world"   : read_csv('hello world.csv')
    'second_csv"   : read_csv('hello world.csv')
    'first_csv'     : lazy(pd.read_csv,{'file':'hello_world'}),
    'second_csv'    : lazy(pd.read_csv, {'file':'second_csv'}),
    'joined'        : lazy(pd.merge, {"left": "first_csv", "right": "second_csv", "on": "key"}),
    'output'        : apply(lambda x: x.to_csv, {'x': 'joined', 'file':'joined.csv'})
})

pipeline.preview() # Default to showing latest node so that when we re/run, it uses cached previous steps.
````

When evaluating a parameter, you should check if it exists as a node, otherwise pass it as a literal?
Otherwise maybe we have to wrap it in a funciton like this:

````python
pipeline = Pipeline() # Only initialize once. 

# This part can be rerun over and over.
# It is idempotent - if the node already exists and the hash is the same, don't replace it.
# If it is tainted, mark it as such so that the next time we need something we know to rerun it.
pipeline.define({
    'hello_world'   : read_csv('hello world.csv')
    'second_csv'    : read_csv('hello world.csv')
    'first_csv'     : lazy(pd.read_csv,{'file': dep('hello_world')}),
    'second_csv'    : lazy(pd.read_csv, {'file': dep('second_csv'})),
    'first_renamed' : lazy(lambda x.rename({'old_name':'new_name'},{'x':dep('first_csv')}))
    'joined'        : lazy(pd.merge, {"left": dep("first_renamed"), "right": dep("second_csv"), "on": "key"}),
    'output'        : apply(lambda x: x.to_csv, {'x': rec('joined'), 'file':'joined.csv'})
})

pipeline.preview() # Show output of all root nodes.

# Function to access output of a particular node
plt.histogram(
    pipeline.get_step('first_csv')
)
````

General algorithm to build DAGs:
Preview shows output of root node(s) - nodes without an outgoing connection. It caches computations of nodes, and uses hashing of the node and it's dependencies to determine if it is tained and needs re-ran.

Read in nodes. Order nodes. Figure out which can be executed concurrently.

### Todo
* Create class to represent nodes: `lazy`
    * Dictionary of dependencies of other nodes
    * Dictionary of static input nodes
    * Hash of inputs (that includes hash of upstream nodes)
    * Cached output
    * Whether or not it is tained - boolean
* Create pipeline class: `pipeline`
    * Dictionary to store nodes
    * Topological ordering of nodes
    * Method to get output
    * Method to check if any nodes are tainted
    * Method to add nodes. If they are already there, check if it is tainted and mark it. Otherwise don't do anything.
* Create dependency class: `dep`
    * Class that represents a dependency on a previous node.


## Implementation

### Pipeline Class `pipeline`
* Init: Define empty pipeline

* Add first pipeline.
    1. Parse pipeline into tree.
    2. Check for cycles, fail if so.
    3. Create topological execution order and save to class.

* Update pipeline
    1. If pipeline is empty:
        1. Parse pipeline into tree.
        2. Check for cycles, fail if so.
        3. Create topological execution order and save to class.
    2. Check if input is a valid tree.
    3. Check for and add any new nodes that aren't already existing.
    4. Compare existing nodes to new nodes via hashes to see if they are the same. If not, mark them as tainted. 
    5. Taint all downstream nodes of tainted nodes (recursivly).

* Get data from node.
    1. Check if that node is cached and not tainted.
    2. If so, return that data (frame).
    3. If not, evaluate that node.
    4. Return result.

### Node Class `lazy`
* Init: Create and store function and parameters.
* Compare (Node)
    1. Hash parameters, fuction of self and other node.
    2. If they are the same, return them.

* Hash
    1. Create hash of self.function, self.params.
    2. Return the hash.

* Eval
    1. Check for any parameters that are nodes. If there are, eval that node.
    2. If not tainted, return result.
    3. Else:
    4. Fun function, passing parameters.
    5. Cache result. Change self.tainted to False.
    6. Return result.

### File Class `lazy_read_csv`
* Init: Store file name, set hash to empty.
* Eval
    1. Check if filehash matches
    2. If so, return cached dataframe
    1. Else, read in dataframe and cache
    2. Store new filehash

### Dependency Class `dep`
* Init: Save string of dependency
**I don't think we need more than this. This is just so we can tell when a parameter is from another node.**