import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j._

object Test {

  case class Point ( X: Double, Y: Double )

  def distance ( x: Point, y: Point ): Double
    = Math.sqrt(Math.pow(x.X-y.X,2)+Math.pow(x.Y-y.Y,2))

  def main ( args: Array[String] ) {
    val iterations = args(0).toInt
    val centroid_file = args(1)
    val point_file = args(2)
    val sparkConf = new SparkConf().setAppName("Spark DataFrame KMeans")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    sparkConf.set("spark.logConf","false")
    sparkConf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    sqlContext.setConf("spark.sql.shuffle.partitions",args(3))

    val t: Long = System.currentTimeMillis()

    var centroids = spark.sparkContext.textFile(centroid_file)
                         .map(_.split(",")).map(n => Point(n(0).toDouble,n(1).toDouble))
                         .collect()

    val points = spark.sparkContext.textFile(point_file)
                      .map(_.split(",")).map(n => Point(n(0).toDouble,n(1).toDouble))
                      .toDF()

    val get_closest_centroid
      = udf( ( x: Double, y: Double ) => { val p = new Point(x,y)
                                           centroids.map(c => (distance(p,c),c)).sortBy(_._1).head._2 } )

    for (i <- 1 to iterations) {
      println(s"Iteration $i")
      centroids = points.groupBy(get_closest_centroid(col("X"),col("Y")))
                        .agg(avg("X"),avg("Y"))
                        .rdd.map { case Row(_,x:Double,y:Double) => Point(x,y) }.collect()
    }

    centroids.map(println)

    println("**** Spark DataFrame run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }
}
