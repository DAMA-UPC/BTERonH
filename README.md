# BTERonH
A hadoop implementation of the BTER graph generation algorithm based on that in http://epubs.siam.org/doi/abs/10.1137/130914218

## Compiling

```
mvn assembly:assembly
```

## Running

For standalone hadoop:

```
java -cp target/bteronh-0.0.1-jar-with-dependencies.jar ldbc.snb.bteronh.BTERMain -P params.ini
```
For pseudo-/distributed hadoop:

```
hadoop jar target/bteronh-0.0.1-jar-with-dependencies.jar ldbc.snb.bteronh.BTERMain -P params.ini
```
These commands produce a file edge\_0 in folder ./data/data

## Configuration

Supports the following options, which can be passed via the params.ini files using the -P option, or via command line using the -p option (-p "option:value").

* ldbc.snb.bteronh.generator.numThreads:X -- The number of threads to use. This also determines the number of output edge\_* files.
* ldbc.snb.bteronh.generator.numNodes:X -- The number of nodes in the resulting graph
* ldbc.snb.bteronh.generator.seed:X -- The seed used to generate the graph.
* ldbc.snb.bteronh.serializer.workspace:X -- The folder where data will be output
* ldbc.snb.bteronh.serializer.outputFileName:X -- The output file name 
* ldbc.snb.bteronh.generator.degreeSequence:X -- The file containing the degree sequence to reproduce. For examples see src/main/resources/degreeSequences
* ldbc.snb.bteronh.generator.ccPerDegree:X -- The file containing the list of avg. clustering coefficient per degree. For examples see src/main.resources/CCs

## Tools

### DegreeDistribution
There is a tool for computing the degree distribution in a distributed way using hadoop, which is specially useful for large graphs.

```
hadoop jar target/bteronh-0.0.1-jar-with-dependencies.jar ldbc.snb.bteronh.tools.DegreeDistribution -inputfiles <list of edge list files> -outputfolder <hdfs folder where data will be output>
```
The tool will output several files containing the degree distribution. The tool accepts three options
* -inputfiles -- A list of files containing one edge per line (without repeated edges)
* -outputfolder -- The hdfs folder where the distribution will be output
* -numthreads -- Number of threads to use for the reducing phase

