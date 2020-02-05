import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import scala.util.Random


object ConditionalSum {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toLong

    val conf = new SparkConf().setAppName("ConditionalSum")
    val sc = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val rand = new Random()

    val V = sc.parallelize(1L to length/100)
              .flatMap{ i => (1 to 100).map{ j => rand.nextDouble()*200 } }
              .cache()

    println("*** %d  %.2f GB".format(length,length.toDouble*8/(1024.0*1024.0*1024.0)))

    def test () {
      var t: Long = System.currentTimeMillis()

      try {
      println(V.filter( _ < 100).reduce(_+_))

      println("**** ConditionalSumSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""
         var sum: Double = 0.0;

         for v in V do
             if (v < 100)
                sum += v;

         println(sum);
        """)

      println("**** ConditionalSumDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x); }

      t = System.currentTimeMillis()

      try {
      import org.apache.spark.api.java.JavaRDD
      println(ConditionalSumCasper.sumList(new JavaRDD(V.map(Double.box))))

      println("**** ConditionalSumCasper run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
    }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}
