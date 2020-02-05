import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import scala.util.Random
import org.apache.spark.api.java.JavaRDD
import generated.phoenix._


object WordCount {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toLong

    val conf = new SparkConf().setAppName("WordCount")
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
      val words = V

      val key1 = "key1"
      val key2 = "key2"
      val key3 = "key3"

      var t: Long = System.currentTimeMillis()

      try {
      val counts = words.map((_,1)).reduceByKey(_+_)

      println(counts.count())

      println("**** WordCountSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""

         var C: map[String,Int] = map();

         for w in words do
             C[w] += 1;

         println(C.count);

        """)

      println("**** WordCountDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      WordCountCasper.countWords(new JavaRDD(V))

      println("**** WordCountCasper run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
    }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}
