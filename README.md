# DIQL: A Data Intensive Query Language

DIQL (the Data-Intensive Query Language) is a query language for DISC
(Data-Intensive Scalable Computing) systems, that is deeply embedded
in Scala. The DIQL compiler optimizes DIQL queries and translates
them to Java byte code at compile-time. DIQL is designed to support
multiple Scala-based APIs for distributed processing by abstracting
their distributed data collections as a DataBag, which is a bag
distributed across the worker nodes of a computer cluster. Currently,
DIQL supports three Big Data platforms that provide different APIs and
performance characteristics: [Apache Spark](http://spark.apache.org/),
[Apache Flink](http://flink.apache.org/), and [Twitter
Cascading/Scalding](https://github.com/twitter/scalding). Unlike other query
languages for DISC systems, DIQL can uniformly work on both
distributed and in-memory collections using the same syntax. DIQL
allows seamless mixing of native Scala code, which may contain UDF
calls, with SQL-like query syntax, thus combining the flexibility of
general-purpose programming languages with the declarativeness of
database query languages. DIQL queries may use any Scala pattern, may
access any Scala variable, and may embed any Scala code without any
marshaling. More importantly, DIQL queries can use the core Scala
libraries and tools as well as user-defined classes without having to
add any special declaration. This tight integration with Scala
minimizes impedance mismatch, reduces program development time, and
increases productivity, since it finds syntax and type errors at
compile-time. DIQL supports nested collections and hierarchical data,
and allows query nesting at any place in a query. The query optimizer
can find any possible join, including joins hidden across deeply
nested queries, thus unnesting any form of query nesting.

## Installation:

DIQL requires Scala 2.11, and at least one of the following 3 DISC platforms:
Apache Spark, Apache Flink, Twitter Cascading/Scalding.

### Installation on Spark

To compile DIQL using scala 2.11.7 and Spark core 2.4.0, use:
```bash
mvn install
```
For different Scala/Spark versions, use for example:
```bash
mvn -Dscala.version=2.11.1 -Dspark.version=1.6.2 install
```
To test few DIQL queries on Spark:
```bash
export SPARK_HOME= ... path to Spark home ...
cd tests/spark
./build test.scala
./run
```

### Installation on Flink

To compile DIQL using scala 2.11.7 and Flink 1.6.2, use:
```bash
mvn -f pom-flink.xml install
```
For different Scala/Flink versions, use for example:
```bash
mvn -f pom-flink.xml -Dscala.version=2.11.1 -Dflink.version=1.1.0 install
```
To test few DIQL queries on Flink:
```bash
export FLINK_HOME= ... path to Flink home ...
${FLINK_HOME}/bin/start-cluster.sh
cd tests/flink
./build test.scala
./run
```

### Installation on Scalding

To compile DIQL using scala 2.11.7, Scalding 0.17.2, and Cascading 3.3.0, use:
```bash
mvn -f pom-scalding.xml install
```
To test few DIQL queries on Scalding:
```bash
export SCALDING_HOME= ... path to Scalding home ...
cd tests/scalding
./build test.scala
./run
```
The results of run are stored in the directory `results`.

## Macros:

DIQL syntax          | meaning
---------------------|-------------------------------------------------------
`explain(true)`        | to get more information about optimization and compilation steps
`monoid("+",0)`      | to define a new monoid for an infix operation
`q(""" ... """)`     | compile a DIQL query to Scala code
`debug(""" ... """)`     | compile a DIQL query to Scala code (with debugging)
`qs(""" ... """)`    | compile many DIQL queries to code that returns `List[Any]`
`m(""" ... """)`    | define macros (functions that are expanded at compile-time)

## Data model

DIQL queries can work on both distributed and regular Scala
collections using the same syntax. A distributed collection (called a DataBag)
is an immutable homogeneous collection of data distributed across the
worker nodes of a cluster. They are supported on various distributed
platforms under different names: RDDs in Spark, and DataSets in Flink,
and TypedPipes in Scalding. The generator and aggregation domains in
DIQL queries must conform to the types DataBag, Traversable, or Array,
where a DataBag is an RDD class in Spark, a DataSet class in Flink, or
a TypedPipe class in Scalding. That is, these domains must be
collections of type T that satisfies `T <: DataBag[_]`,
`T <: Traversable[_]`, or `T <: Array[_]`.

## Query syntax:

A DIQL query is any functional Scala expression extended with the following query syntax:

### DIQL expressions:
```
e ::=  any functional Scala expression (no blocks, no val/var declarations, no assignments)
    |  select [ distinct ] e             (select-query)
       from q,...,q
       [ where e ]
       [ group by p [ : e ] 
            [ from q,...,q [ where e ]
              group by p [ : e ] ]       (the right branch of a coGroup)
            [ having e ] ]
       [ order by e ]
    |  some q,...,q: e                    (existential quantification)
    |  all q,...,q: e                     (universal quantification)
    |  e member e                         (membership testing)
    |  e union e                          (bag union)
    |  e intersect e                      (bag intersection)
    |  e minus e                          (bag difference)
    |  let p = e, ..., p = e in e         (let-binding)
    |  +/e                                (aggregation using the monoid +)
    |  repeat p = e step e
       [ until e ] [ limit n ]            (repetition)
    |  trace(e)                           (trace the evaluation of e)
```
### DIQL patterns:
```
p ::= any Scala pattern (including refutable patterns)
```
### DIQL qualifiers:
```
q ::=  p <- e                 (generator over a DataBag or an Iterable sequence)
    |  p <-- e                (like p <- e but for a small dataset)
    |  p = e                  (binding)
```
### Macros:
A macro is a function with a DIQL body that is expanded at query translation time. Macros must be defined inside `m("""...""")`. Syntax:
```
def name ( v: type, ..., v: type ) = e
```
## Example:
```scala
    q("""
      select (i/2.0,z._2,max/xs)
        from (i,3,xs) <- S,
             x <- xs,
             z <- (select (i,+/j) from (i,j,_) <- S group by i)
        where (some k <- xs: k>3) && i==z._1
        order by avg/xs desc
    """)
```

## k-means example:
```scala
    case class Point ( X: Double, Y: Double )

    def distance ( x: Point, y: Point ): Double
      = Math.sqrt(Math.pow(x.X-y.X,2)+Math.pow(x.Y-y.Y,2))

    q("""let points = sc.textFile("points.txt")
                        .map( _.split(",") )
                        .map( p => Point(p(0).toDouble,p(1).toDouble) )
         in repeat centroids = Array( Point(0,0), Point(10,0), Point(0,10), Point(10,10) )
            step select Point( avg/x, avg/y )
                 from p@Point(x,y) <- points
                 group by k: ( select c
                               from c <- centroids
                               order by distance(c,p) ).head
            limit 10
      """).map(println)
```
## PageRank example:
```scala
    case class GraphNode ( id: Long, rank: Double, adjacent: List[Long] )
    case class PageRank ( id: Long, rank: Double )

    val graph_size = 1000
    val factor = 0.85

   q("""
      select PageRank( id = x.id, rank = x.rank )
      from x <- ( repeat graph = select GraphNode( id = n.toLong,
                                                   rank = 0.5D,
                                                   adjacent = ns.map(_.toLong) )
                                 from line <- sc.textFile("graph.txt"),
                                      n::ns = line.split(",").toList
                  step select GraphNode( id = m.id, rank = n.rank, adjacent = m.adjacent )
                       from n <- (select PageRank( id = key,
                                                   rank = (1-factor)/graph_size
                                                          +factor*(+/select x.rank from x <- c) )
                                  from c <- ( select PageRank( id = a,
                                                               rank = n.rank/(count/n.adjacent) )
                                              from n <- graph,
                                                   a <- n.adjacent )
                                  group by key: c.id),
                            m <- graph
                       where n.id == m.id
                  limit 10 )
      order by (x.rank) desc
     """).foreach(println)
```
