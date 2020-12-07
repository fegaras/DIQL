import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random

case class Point(x: Double, y: Double)

object LinearRegression {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toInt
    val conf = new SparkConf().setAppName("LinearRegression")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val rand = new Random()
    val P = sc.parallelize(1L to length/100)
              .flatMap{ i => (1 to 100).map{ j => val x = rand.nextDouble()*1000
                                                  val dx = rand.nextDouble()*10
                                                  Point(x+dx,x-dx)}}
		.zipWithIndex.map{case (line,i) => (i, line)}
              .cache()
    val size = sizeof(1L,Point(1.0D,1.0D))
    println("*** %d %.2f GB".format(length, length.toDouble*size/(1024.0*1024.0*1024.0)))
    val Pds = P.toDS()
    Pds.createOrReplaceTempView("Pds")
    val n = Pds.count

    def test () {
      var t: Long = System.currentTimeMillis()

      try {
         val x_bar = P.map(_._2.x).reduce(_+_)/n
   	 val y_bar = P.map(_._2.y).reduce(_+_)/n

   	 val xx_bar = P.map(x => (x._2.x - x_bar)*(x._2.x - x_bar)).reduce(_+_)
   	 val yy_bar = P.map(y => (y._2.y - y_bar)*(y._2.y - y_bar)).reduce(_+_)
   	 val xy_bar = P.map(p => (p._2.x - x_bar)*(p._2.y - y_bar)).reduce(_+_)

   	 val slope = xy_bar/xx_bar
   	 val intercept = y_bar - slope * x_bar

   	 println(slope)
   	 println(intercept)
      println("**** SparkRDD run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""
       var sum_x: Double = 0.0;
      var sum_y: Double = 0.0;
      var x: Double = 0.0;
      var y: Double = 0.0;
      var s: Double = 0.0;
      var q: Double = 0.0;
      var r: Double = 0.0;
      var slope: Double = 0.0;
      var intercept: Double = 0.0;

      for i = 0, n-1 do {
          sum_x += P[i].x;
          sum_y += P[i].y;
      };

     x := sum_x/n;
     y := sum_y/n;

     for i = 0, n-1 do {
           s += (P[i].x - x)*(P[i].x - x);
           q += (P[i].y - y)*(P[i].y - y);
           r += (P[i].x - x)*(P[i].y - y);
     };

     slope := r/s;
     intercept := y - slope*x;
     println(slope);
     println(intercept);
        """)

      println("**** Diablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
   
      t = System.currentTimeMillis()

      try {
      s(sc,"""
        var sum_x: Double = 0.0;
      var sum_y: Double = 0.0;
      var x: Double = 0.0;
      var y: Double = 0.0;
      var s: Double = 0.0;
      var q: Double = 0.0;
      var r: Double = 0.0;
      var slope: Double = 0.0;
      var intercept: Double = 0.0;

      for i = 0, n-1 do {
          sum_x += Pds[i].x;
          sum_y += Pds[i].y;
      };

     x := sum_x/n;
     y := sum_y/n;

     for i = 0, n-1 do {
           s += (Pds[i].x - x)*(Pds[i].x - x);
           q += (Pds[i].y - y)*(Pds[i].y - y);
           r += (Pds[i].x - x)*(Pds[i].y - y);
     };

     slope := r/s;
     intercept := y - slope*x;
     println(slope);
     println(intercept); 
        """)

      println("**** SQLGen run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()
      try{
      var sum_x = spark.sql("SELECT SUM(Pds._2.x) AS _2 FROM Pds")
    sum_x.createOrReplaceTempView("sum_x")
    val sum_y = spark.sql("SELECT SUM(Pds._2.y) AS _2 FROM Pds")
    sum_y.createOrReplaceTempView("sum_y")

    val x_bar = sum_x.head().getDouble(0)/n
    val y_bar = sum_y.head().getDouble(0)/n

    val xx_bar = spark.sql(s"SELECT SUM((Pds._2.X - $x_bar)*(Pds._2.X - $x_bar)) AS _1 FROM Pds")
    val yy_bar = spark.sql(s"SELECT SUM((Pds._2.Y - $y_bar)*(Pds._2.Y - $y_bar)) AS _1 FROM Pds")
    val xy_bar = spark.sql(s"SELECT SUM((Pds._2.X - $x_bar)*(Pds._2.Y - $y_bar)) AS _1 FROM Pds")

    val slope= xy_bar.head().getDouble(0)/xx_bar.head().getDouble(0)
    val intercept = y_bar - slope*x_bar
   println(slope)
   println(intercept)
      println("**** SparkSQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      }catch { case x: Throwable => println(x) }
 }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}

