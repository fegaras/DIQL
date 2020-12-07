import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.util.Random


case class GB ( K: Long, A: Double )

object GroupBy {

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
 
    val GBsize = sizeof(GB(1L,1.0D))
    println("*** %d  %.2f GB".format(length,length.toDouble*GBsize/(1024.0*1024.0*1024.0)))

    val V = sc.parallelize(1L to length/100)
              .flatMap{ i => (1 to 100).map{ j => GB( Math.abs(rand.nextDouble()*max).toLong,
                                                      rand.nextDouble())}}
              .zipWithIndex.map{case (line,i) => (i, line)}
              .cache()

    val Vds = V.toDS()
    Vds.createOrReplaceTempView("Vds")

    val n = Vds.count()

    def test () {
      var t: Long = System.currentTimeMillis()

      try {
      val C = V.map{ case (_, GB(k,v)) => (k,v) }.reduceByKey(_+_)

      println(C.count)

      println("**** GroupBySpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""

         var C: vector[Double] = vector();

         for i = 0, n-1 do
            C[V[i].K] += V[i].A;

         println(C.count);

     
        """)

      println("**** GroupByDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
    
    
    t = System.currentTimeMillis()

      try{
       s(sc,"""

        var C: vector[Double] = vector();

        for i = 0, n-1 do
       	  C[Vds[i].K] += Vds[i].A;

        println(C.count);

     """)
         println("**** GroupBySql run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
	} catch { case x: Throwable => println(x) }

    t = System.currentTimeMillis()

      try{
       var C = spark.sql("SELECT Vds._2.K AS _1, SUM(Vds._2.A) AS _2 FROM Vds GROUP BY Vds._2.K");
       C.createOrReplaceTempView("C");
       println(C.count())
       println("**** GroupBySparkSql run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
}
    for ( i <- 1 to repeats )
        test()
    sc.stop()
  }
}
