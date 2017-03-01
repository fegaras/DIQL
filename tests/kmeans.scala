import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("k-means")
    val sc = new SparkContext(conf)

    debug(true)

    case class Point ( X: Double, Y: Double )

    def distance ( x: Point, y: Point ): Double
      = Math.sqrt(Math.pow(x.X-y.X,2)+Math.pow(x.Y-y.Y,2))

    val points = sc.textFile("points.txt")
                   .map( _.split(",") )
                   .map( p => Point(p(0).toDouble,p(1).toDouble) )

    var centroids = Array( Point(0,0), Point(10,0), Point(0,10), Point(10,10) )

    for ( i <- 1 to 10 )
       centroids = q("""
                     select Point( avg/x, avg/y )
                     from p@Point(x,y) <- points
                     group by k: ( select c
                                   from c <- centroids
                                   order by distance(c,p) ).head
                   """).collect

    centroids.map(println(_))
  }
}
