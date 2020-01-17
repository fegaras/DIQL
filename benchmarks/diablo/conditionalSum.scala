import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random


object ConditionalSum {
  val conf = new SparkConf().setAppName("ConditionalSum")
  val sc = new SparkContext(conf)
  val fs = FileSystem.get(sc.hadoopConfiguration)

  conf.set("spark.logConf","false")
  conf.set("spark.eventLog.enabled","false")
  LogManager.getRootLogger().setLevel(Level.WARN)

  def build ( length: Int, file: String ) {
    val rand = new Random()

    def d () = rand.nextDouble()*200.0

    sc.parallelize(1 to length,1)
      .map{ x => d()+","+d() }
      .saveAsTextFile(file)
  }

  def test ( file: String ) {
    var V = sc.textFile(file).flatMap(_.split(",").toSeq).map(_.toDouble)

    var t: Long = System.currentTimeMillis()

    println(V.filter( _ < 100).reduce(_+_))

    println("**** ConditionalSumSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    v(sc,"""
      var sum: Double = 0.0;

      for v in V do
          if (v < 100)
             sum += v;

      println(sum);
     """)

    println("**** ConditionalSum run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
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
