import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random


object LinearRegression {
  val conf = new SparkConf().setAppName("LinearRegression")
  val sc = new SparkContext(conf)
  val fs = FileSystem.get(sc.hadoopConfiguration)

  conf.set("dfs.replication","1")
  conf.set("spark.logConf","false")
  conf.set("spark.eventLog.enabled","false")
  LogManager.getRootLogger().setLevel(Level.WARN)

  def build ( length: Int, file: String ) {
    val rand = new Random()

    sc.parallelize(1 to length,1)
      .map{ n => val x = rand.nextDouble()*1000
                 val dx = rand.nextDouble()*10
                 (x+dx)+","+(x-dx)
          }.saveAsTextFile(file)
  }

  def test ( file: String ) {
    val P = sc.textFile(file)
              .map( line => { val a = line.split(",")
                              (a(0).toDouble, a(1).toDouble) } )

    val n = P.count()

    var t: Long = System.currentTimeMillis()

    val x_bar = P.map(_._1).reduce(_+_)/n
    val y_bar = P.map(_._2).reduce(_+_)/n

    val xx_bar = P.map(x => (x._1 - x_bar)*(x._1 - x_bar)).reduce(_+_)
    val yy_bar = P.map(y => (y._2 - y_bar)*(y._2 - y_bar)).reduce(_+_)
    val xy_bar = P.map(p => (p._1 - x_bar)*(p._2 - y_bar)).reduce(_+_)

    val slope = xy_bar/xx_bar
    val intercept = y_bar - slope * x_bar

    println(slope+" "+intercept)

    println("**** LinearRegressionSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    v(sc,"""

      var sum_x: Double = 0.0;
      var sum_y: Double = 0.0;
      var x_bar: Double = 0.0;
      var y_bar: Double = 0.0;
      var xx_bar: Double = 0.0;
      var yy_bar: Double = 0.0;
      var xy_bar: Double = 0.0;
      var slope: Double = 0.0;
      var intercept: Double = 0.0;

      for p in P do {
          sum_x += p._1;
          sum_y += p._2;
      };

      x_bar := sum_x/n;
      y_bar := sum_y/n;

      for p in P do {
          xx_bar += (p._1 - x_bar)*(p._1 - x_bar);
          yy_bar += (p._2 - y_bar)*(p._2 - y_bar);
          xy_bar += (p._1 - x_bar)*(p._2 - y_bar);
     };
     
     slope := xy_bar/xx_bar;
     intercept := y_bar - slope*x_bar;

     println(slope+" "+intercept);

     """)

    println("**** LinearRegression run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toInt
    val file = "data"

    fs.delete(new Path(file),true)
    build(length,file)
    println("*** %d  %.2f GB".format(length,fs.getContentSummary(new Path(file)).getLength()/(1024.0*1024.0*1024.0)))

    for ( i <- 1 to repeats )
        test(file)

    fs.delete(new Path(file),true)
    sc.stop()
  }
}
