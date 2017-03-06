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
    |  repeat p = e step e            (repetition)
       [ until e ] [ limit n ]
    |  let p = e in e                 (let-binding)
    |  +/e                            (aggregation using the monoid +)
```
### DIQL patterns:
```
p ::= any Scala pattern
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

    val points = sc.textFile("points.txt")
                  .map( line => { val List(x,y) = line.split(",").toList
                                  Point(x.toDouble,y.toDouble) 
                                } )

    var centroids = List( Point(0,0), Point(10,0), Point(0,10), Point(10,10) )

    for ( i <- 1 to 10 )
       centroids = q("""
          select Point( avg/x, avg/y )
          from p@Point(x,y) <- points
          group by k: (select c from c <- centroids order by distance(c,p)).head
       """).collect.toList
```
