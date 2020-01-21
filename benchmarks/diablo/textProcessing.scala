import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import scala.util.Random
import org.apache.spark.api.java.JavaRDD
import generated.phoenix._


object TextProcessing {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toLong

    val conf = new SparkConf().setAppName("TextProcessing")
    val sc = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val rand = new Random()

    val max: Long = length/10   // 10 duplicates on the average

    val V = sc.parallelize(1L to length/100)
              .flatMap{ i => (1 to 100).map{ j => "x"+(Math.abs(rand.nextLong())%max) } }
              .cache()

    val size = V.map(_.length).reduce(_+_)

    println("*** %d  %.2f GB".format(length,size/(1024.0*1024.0*1024.0)))

    def test () {
      val x = V.first()

      var t: Long = System.currentTimeMillis()

      println(V.filter(_ != x).isEmpty)

      println("**** EqualSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

      t = System.currentTimeMillis()

      v(sc,"""
         var eq: Boolean = true;

         for v in V do
             eq := eq && v == x;

         println(eq);

        """)

      println("**** EqualDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

      t = System.currentTimeMillis()

      println(EqualCasper.equal(new JavaRDD(V)))

      println("**** EqualCasper run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

      val words = V

      val key1 = "key1"
      val key2 = "key2"
      val key3 = "key3"

      t = System.currentTimeMillis()

      words.flatMap{ w => List((1,w == key1),(2,w == key2),(3,w == key3)) }
           .reduceByKey(_||_).foreach(println)

      println("**** StringMatchSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

      t = System.currentTimeMillis()

      v(sc,"""

         var c1: Boolean = false;
         var c2: Boolean = false;
         var c3: Boolean = false;

         for w in words do {
             c1 := c1 || w == key1;
             c2 := c2 || w == key2;
             c3 := c3 || w == key3;
         };

         println(c1+" "+c2+" "+c3);

        """)

      println("**** StringMatchDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

      t = System.currentTimeMillis()

      StringMatchCasper.matchWords(new JavaRDD(V)).foreach(println)

      println("**** StringMatchCasper run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

      t = System.currentTimeMillis()

      val counts = words.map((_,1)).reduceByKey(_+_)

      println(counts.count())

      println("**** WordCountSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

      t = System.currentTimeMillis()

      v(sc,"""

         var C: map[String,Int] = map();

         for w in words do
             C[w] += 1;

         println(C.count);

        """)

      println("**** WordCountDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

/*    Casper crashes
      t = System.currentTimeMillis()

      println(WordCountCasper.countWords(new JavaRDD(V)).size())

      println("**** WordCountCasper run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
*/
    }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}
