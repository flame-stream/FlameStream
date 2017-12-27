# Benchmarks setup

There four executables that take part in benchmarking

1. Worker
2. Graph deployer
3. Benchstand
4. ZooKeeper

#### Worker

Runs on each machine, listens for changes in zookeeper

#### Graph deployer

One shot executable: it starts, connects to the ZK, deploys graph and fronts, dies.

#### Benchstand

The core component of the benchmarks. It submits items to the graph and collects the results.

- Starts
- Reads the data to be processed from filesystem
- Creates sockets for sending and receiving data
- Waits until the sufficient number of "fronts" (of Flinks sources) connects to the graph.
- Submits items to the graph, collects results and measures the latencies.

#### Jars

- `worker.jar` - the classes of worker itself
- `business-logic.jar` - the jar that contains the graphs and business logic of benchmarking, 
to be injected into classpath of the worker
- `graph-deployer.jar` - the `void main(String[])` deployer
- `benchstand.jar` - the benchstand

The last three items could be the one module with multiple jar packages

# Measurements

1. Inverted index of 1, 2, 4, 8, 10 workers
2. Single filter
3. Single grouping test (suffling) `// there shouldn't be any replay with window 1`
