import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import scala.util.Random


case class GB ( K: Long, A: Double )

object GroupBy {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toLong

    val conf = new SparkConf().setAppName("GroupBy")
    val sc = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val rand = new Random()

    val max: Long = length/10   // 10 duplicates on the average
 
    val GBsize = sizeof(GB(1L,1.0D))
    println("*** %d  %.2f GB".format(length,length*GBsize/(1024.0*1024.0*1024.0)))

    val V = sc.parallelize(1L to length/100)
              .flatMap{ i => (1 to 100).map{ j => GB( Math.abs(rand.nextDouble()*max).toLong,
                                                      rand.nextDouble() ) } }
              .cache()

    def test () {
      var t: Long = System.currentTimeMillis()

      val C = V.map{ case GB(k,v) => (k,v) }.reduceByKey(_+_)

      println(C.count())

      println("**** GroupBySpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

      t = System.currentTimeMillis()

      v(sc,"""

         var C: vector[Double] = vector();

         for v in V do
            C[v.K] += v.A;

         println(C.count);
     
        """)

      println("**** GroupByDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    }
 
    for ( i <- 1 to repeats )
        test()
    sc.stop()
  }
}
