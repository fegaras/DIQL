import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._

object Test {

  case class Point ( X: Double, Y: Double )

  def distance ( x: Point, y: Point ): Double
    = Math.sqrt(Math.pow(x.X-y.X,2)+Math.pow(x.Y-y.Y,2))

  def main ( args: Array[String] ) {
    val iterations = args(0).toInt
    val centroid_file = args(1)
    val point_file = args(2)
    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    explain(true)
    val t: Long = System.currentTimeMillis()

    val points = sc.textFile(point_file)
                   .map( _.split(",") )
                   .map( p => Point(p(0).toDouble,p(1).toDouble) )

    val initial_centroids = sc.textFile(centroid_file)
                              .map( _.split(",") )
                              .map( p => Point(p(0).toDouble,p(1).toDouble) )
                              .collect()

    q(""" repeat centroids = initial_centroids
            step select Point( avg/x, avg/y )
                 from p@Point(x,y) <- points
                 group by k: ( select c
                               from c <- centroids
                               order by distance(c,p) ).head
            limit iterations
      """).map(println)

    sc.stop()

    println("**** DIQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }
}
