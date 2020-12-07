import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.util.Random

object GroupByJoin {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toLong

    val conf = new SparkConf().setAppName("GroupBy")
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

    val max: Long = length/10   // 10 duplicates on the average
 
    val size = sizeof((1L,1.0D))
    println("*** %d  %.2f GB".format(length,length.toDouble*size/(1024.0*1024.0*1024.0)))

    val V = sc.parallelize(1L to length/100)
              .flatMap{ i => (1 to 100).map{ j => (Math.abs(rand.nextDouble()*max).toLong,
                                                      rand.nextDouble())}}
              .cache()

    val K = sc.parallelize(1L to length/100)
              .flatMap{ i => (1 to 100).map{ j =>  Math.abs(rand.nextDouble()*max).toLong}}
              .zipWithIndex.map{case (line,i) => (i, line)}
              .cache()

    val Vds = V.toDS()
    val Kds = K.toDS()

    Vds.createOrReplaceTempView("Vds")
    Kds.createOrReplaceTempView("Kds")

    val n = Vds.count()

    def test () {
      var t: Long = System.currentTimeMillis()

      try {
     	 var C = V.join(K).map{case (_,(v,i)) => (i,v)}
     	 .reduceByKey(_+_)
	println(C.count)
      println("**** SparkRDD run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""

         var C: vector[Double] = vector();

         for i = 0, n-1 do
            C[K[i]] += V[i];

   	 println(C.count); 
        """)

      println("**** Diablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
    
    
    t = System.currentTimeMillis()

      try{
       s(sc,"""

        var C: vector[Double] = vector();

        for i = 0, n-1 do
       	  C[Kds[i]] += Vds[i];

        println(C.count);

     """)
         println("**** SQLGen run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
	} catch { case x: Throwable => println(x) }

    t = System.currentTimeMillis()

      try{
       var C = spark.sql("SELECT Kds._2 AS _1, SUM(Vds._2) AS _2 FROM Kds JOIN Vds ON Kds._1 == Vds._1 GROUP BY Kds._2");
       C.createOrReplaceTempView("C");
       println(C.count);
       println("**** SparkSql run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
}
    for ( i <- 1 to repeats )
        test()
    sc.stop()
  }
}
