import org.apache.spark._
import org.apache.spark.rdd._
import Math._
import org.apache.log4j._

object KMeansSpark {

  type Point = (Double,Double)

  var centroids: Array[Point] = Array[Point]()

  def distance ( x: Point, y: Point ): Double
    = Math.sqrt((x._1-y._1)*(x._1-y._1)+(x._2-y._2)*(x._2-y._2))

  def closest ( p: Point ): Point
    = centroids.minBy(distance(p,_))

  def avg ( s: Iterable[Double] ): Double
    = s.sum/s.size

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("KMeansSpark")
    val sc = new SparkContext(conf)

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val points = sc.textFile(args(0)).map {
                        line => val a = line.split(",")
                                (a(0).toDouble, a(1).toDouble)
                    }

    centroids = sc.textFile(args(1)).map {
                        line => val a = line.split(",")
                                (a(0).toDouble, a(1).toDouble)
                    }.collect()

    val t: Long = System.currentTimeMillis()

    for ( i <- 1 to 10 ) {
      val cs = sc.broadcast(centroids)
      centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }
                        .groupByKey().map {
                              case (c, vs) => (avg(vs.map(_._1)), avg(vs.map(_._2)))
                          }.collect()
    }

    println(centroids.length)

    println("**** KMeansSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    sc.stop()

  }
}
