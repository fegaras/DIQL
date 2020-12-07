import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.util.Random
import org.apache.spark.api.java.JavaRDD

object StringMatch {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toLong

    val conf = new SparkConf().setAppName("StringMatch")
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

    val max: Long = 100000   // 1000 different strings
    val W = sc.parallelize(1L to length/100)
              .flatMap{ i => (1 to 100).map{ j => "x%03d".format(Math.abs(rand.nextInt())%max) } }
               .zipWithIndex.map{ case (line,i) =>  (i, line)}
              .cache()
    val keys = List("x100", "x200", "x300")
    val K = keys.zipWithIndex.map{ case (line,i) =>  (i.toLong, line)}
    val Kbr = sc.broadcast(K)
    val K2 = sc.parallelize(K).collect()

    val Wds = W.toDS()
    val Kds = K.toDS()

    Wds.createOrReplaceTempView("Wds")
    Kds.createOrReplaceTempView("Kds")
    
    val w = Wds.count()
    val k = Kds.count()
    val size = sizeof(1L, "abcd") // 4 chars + length
    println("*** %d  %.2f GB".format(length,length.toDouble*size/(1024.0*1024.0*1024.0)))

    def test () {
      
      var t: Long = System.currentTimeMillis()

      try {
        val R =  W.map{case (k,v) => v}.filter(v => Kbr.value.map{case (k,v) => v}.contains(v))
        println(R.count)
	println("**** SparkRDD run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""
       var C: vector[String] = vector();

       for i = 0, w-1 do {
	  for j = 0, k-1 do
            if (W[i] == K2[j])
              C[j] := K2[j];
     }; 
     println(C.count);
      """)

      println("**** Diablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try{
       s(sc,"""
        var C: vector[String] = vector();

        for i = 0, w-1 do {
 	  for j = 0, k-1 do
            if (Wds[i] == Kds[j])
             C[j] := Kds[j];
       }; 

        println(C.count);

     """)
      println("**** SQLGen run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try{
      var R = spark.sql("SELECT /*+  BROADCAST(Kds) */ Kds._2  FROM Wds JOIN Kds ON Wds._2 == Kds._2");
      R.createOrReplaceTempView("R")
      println(R.count())
      println("**** SparkSQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

    }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}

