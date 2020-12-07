import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.util.Random
import org.apache.spark.api.java.JavaRDD

object WordCount {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toLong

    val conf = new SparkConf().setAppName("WordCount")
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

    val max: Long = 1000   // 1000 different strings

    val V = sc.parallelize(1L to length/100) 
              .flatMap{ i => (1 to 100).map{ j => "x%03d".format(Math.abs(rand.nextInt())%max) } }
              .zipWithIndex.map{case (line,i) => (i, line)}              
              .cache()

    val Vds = V.toDS()
    val n = Vds.count()
    Vds.createOrReplaceTempView("Vds")
    val size = sizeof(1L, "abcd")
    println("*** %d  %.2f GB".format(length,length.toDouble*size/(1024.0*1024.0*1024.0)))

    def test () {
      
      var t: Long = System.currentTimeMillis()

      try {
      val counts = V.map{case (_,w) => (w,1)}.reduceByKey(_+_)

      println(counts.count())

      println("**** SparkRDD run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""

         var C: map[String,Int] = map();

         for i = 0, n-1 do
             C[V[i]] += 1;

         println(C.count);

        """)

      println("**** Diablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try{
       s(sc,"""

      var C: map[String,Int] = map();

      for i = 0, n-1 do
           C[Vds[i]] += 1;
      
      println(C.count);
     """)
      println("**** SQLGen run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try{
      var C = spark.sql("SELECT Vds._2 AS _1, COUNT(Vds._1) AS _2 FROM Vds GROUP BY Vds._2");
      C.createOrReplaceTempView("C")
      println(C.count())
      println("**** SparkSQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

    }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}

