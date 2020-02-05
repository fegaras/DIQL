import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import scala.util.Random
import org.apache.spark.api.java.JavaRDD
import generated.phoenix._


object Equal {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toLong

    val conf = new SparkConf().setAppName("Equal")
    val sc = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val rand = new Random()

    val max: Long = 1000   // 1000 different strings

    val V = sc.parallelize(1L to length/100)
              .flatMap{ i => (1 to 100).map{ j => "x%03d".format(Math.abs(rand.nextInt())%max) } }
              .cache()

    val size = 4+4  // 4 chars + length
    println("*** %d  %.2f GB".format(length,length.toDouble*size/(1024.0*1024.0*1024.0)))

    def test () {
      val x = V.first()
      println(x)

      var t: Long = System.currentTimeMillis()

      try {
        println(V.map(_ == x).reduce(_&&_))

        println("**** EqualSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""
         var eq: Boolean = true;

         for v in V do
             eq := eq && v == x;

         println(eq);

        """)

        println("**** EqualDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
        println(EqualCasper.equal(new JavaRDD(V)))

        println("**** EqualCasper run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
    }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}
