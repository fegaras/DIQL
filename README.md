# DIQL: Data Intensive Query Language

To compile DIQL:
```bash
mvn clean install
```

To test few DIQL queries:
```bash
export SPARK_HOME= ... path to Spark home ...
cd tests
./build test.scala
./run
```

## Macros:

DIQL syntax          | meaning
---------------------|-------------------------------------------------------
`debug(true)`        | to turn debugging on
`monoid("+",0)`      | to define a new monoid for an infix operation
`q(""" ... """)`     | compile a DIQL query to Spark/Scala code
`qs(""" ... """)`    | compile many DIQL queries to code that returns `List[Any]`

## Data model

The generator and aggregation domains in DIQL queries must conform to the types RDD, Traversable, or Array.
That is, they must be collections of type T that satisfies `T <: RDD[_]`, `T <: Traversable[_]`, or `T <: Array[_]`. 

## Query syntax:

A DIQL query is any functional Scala expression extended with the following query syntax:

### DIQL expressions:
```
e ::=  any functional Scala expression (no blocks, no val/var declarations)
    |  select [ distinct] q,...,q [ where e ]
              [ group by p [ : e ] [ having e ] ]
              [ order by e ]
    |  some q,...,q: e                (existential quantification)
    |  all q,...,q: e                 (universal quantification)
    |  e member e                     (membership testing)
    |  e union e                      (bag union)
    |  e intersect e                  (bag intersection)
    |  e minus e                      (bag difference)
    |  let p = e in e                 (let-binding)
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
