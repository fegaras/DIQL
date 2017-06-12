# DIQL: A Data Intensive Query Language

DIQL (the Data-Intensive Query Language) is a query language for DISC (Data-Intensive Scalable Computing) systems, that is deeply embedded in Scala.
The DIQL compiler optimizes DIQL queries and
translates them to Java byte code at compile-time.
The code can run on multiple DISC platforms, currently on Apache Spark and Apache Flink.
Unlike other query
languages for DISC systems, DIQL can uniformly work on any collection
that conforms to the Scala classes RDD or Traversable, thus allowing
one to query both distributed and in-memory collections using the same
syntax. DIQL queries may use any Scala pattern, may access any Scala
variable, and may embed any Scala code, including calls to RDD
methods, such as input/output Spark actions, without having to
introduce any special data format. More importantly, DIQL queries can
use the core Scala libraries and tools as well as user-defined
classes, without having to add any special declaration. This
tight integration with Scala minimizes impedance mismatch. It also
reduces program development time since it finds syntax and type errors
at compile-time. DIQL supports nested collections and hierarchical
data and allows query nesting at any place in a query. The query
optimizer can find any possible join, including joins hidden across
deeply nested queries, thus unnesting any form of query nesting. The
DIQL algebra, which is based on monoid homomorphisms, can capture all
the language features using a very small set of homomorphic
operations. Monoids and monoid homomorphisms directly capture the
most important property required for data parallelism, namely
associativity. They fully support the functionality provided by
current DSLs for DISC processing by directly supporting operations,
such as group-by, order-by, aggregation, and joins between
heterogeneous collections. Currently, the DIQL query optimizer is not
cost-based; instead, it requires a number of hints in a query
to guide the optimizer, such as indicating whether a traversed
collection is small enough to fit in a worker's memory so that the
optimizer may consider using a broadcast join to implement this
traversal.

[Compile-Time Optimization of Embedded Data-Intensive Query Languages](https://lambda.uta.edu/diql.pdf)

## Installation:

DIQL requires Scala 2.11, Apache Spark, and/or Apache Flink.

### Installation on Spark

To compile DIQL using scala 2.11.7 and Spark core 2.1.0, use:
```bash
mvn clean install
```
For different Scala/Spark versions, use for example:
```bash
mvn -Dscala.version=2.11.1 -Dspark.version=1.6.2 clean install
```
To test few DIQL queries on Spark:
```bash
export SPARK_HOME= ... path to Spark home ...
cd tests/spark
./build test.scala
./run
```
### Installation on Flink

To compile DIQL using scala 2.11.7 and Flink 1.2.0, use:
```bash
mvn -f pom-flink.xml clean install
```
For different Scala/Flink versions, use for example:
```bash
mvn -f pom-flink.xml -Dscala.version=2.11.1 -Dflink.version=1.1.0 clean install
```
To test few DIQL queries on Flink:
```bash
export FLINK_HOME= ... path to Flink home ...
${FLINK_HOME}/bin/start-local.sh
cd tests/flink
./build test.scala
./run
```

## Macros:

DIQL syntax          | meaning
---------------------|-------------------------------------------------------
`explain(true)`        | to get more information about optimization and compilation steps
`monoid("+",0)`      | to define a new monoid for an infix operation
`q(""" ... """)`     | compile a DIQL query to Scala code
`qs(""" ... """)`    | compile many DIQL queries to code that returns `List[Any]`
`m(""" ... """)`    | define macros (functions that are expanded at compile-time)

## Data model

The generator and aggregation domains in DIQL queries must conform to the types RDD, Traversable, or Array.
That is, they must be collections of type T that satisfies `T <: RDD[_]`, `T <: Traversable[_]`, or `T <: Array[_]`. 

## Query syntax:

A DIQL query is any functional Scala expression extended with the following query syntax:

### DIQL expressions:
```
e ::=  any functional Scala expression (no blocks, no val/var declarations)
    |  select [ distinct ] e
       from q,...,q
       [ where e ]
       [ group by p [ : e ] 
            [ from q,...,q [ where e ] group by p [ : e ] ]
            [ having e ] ]
       [ order by e ]
    |  some q,...,q: e                (existential quantification)
    |  all q,...,q: e                 (universal quantification)
    |  e member e                     (membership testing)
    |  e union e                      (bag union)
    |  e intersect e                  (bag intersection)
    |  e minus e                      (bag difference)
    |  let p = e, ..., p = e in e     (let-binding)
    |  +/e                            (aggregation using the monoid +)
    |  repeat p = e step e            (repetition)
       [ until e ] [ limit n ]
```
### DIQL patterns:
```
p ::= any Scala pattern (including refutable patterns)
```
### DIQL qualifiers:
```
q ::=  p <- e                 (generator over an RDD or an Iterable sequence)
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
