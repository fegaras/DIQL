import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import scala.util.Random


case class Color ( red: Int, green: Int, blue: Int )


object Histogram {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toLong

    val conf = new SparkConf().setAppName("Histogram")
    val sc = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val rand = new Random()

    def byte () = Math.abs(rand.nextInt()) % 256

    val P = sc.parallelize(1L to length/100)
              .flatMap{ i => (1 to 100).map{ j => Color(byte(),byte(),byte()) } }
              .cache()

    val size = sizeof(Color(1,1,1))
    println("*** %d  %.2f GB".format(length,length*size/(1024.0*1024.0*1024.0)))

    def test () {
      var t: Long = System.currentTimeMillis()

      val R = P.map(_.red).countByValue()
      val G = P.map(_.green).countByValue()
      val B = P.map(_.blue).countByValue()

      println(R.size)
      println(G.size)
      println(B.size)

      println("**** HistogramSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

      t = System.currentTimeMillis()

      v(sc,"""

         var R: map[Int,Int] = map();
         var G: map[Int,Int] = map();
         var B: map[Int,Int] = map();

         for p in P do {
             R[p.red] += 1;
             G[p.green] += 1;
             B[p.blue] += 1;
         };

         println(R.count);
         println(G.count);
         println(B.count);

        """)

      println("**** HistogramDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}
