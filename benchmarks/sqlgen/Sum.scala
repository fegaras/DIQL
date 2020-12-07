import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.util.Random

object Sum {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toLong

    val conf = new SparkConf().setAppName("Sum")
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

    val V = sc.parallelize(1L to length/100)
               .flatMap{ i => (1 to 100).map{ j => rand.nextDouble()*200 } }
		.zipWithIndex.map{case (line,i) => (i, line)}
   		.cache()
 
    val Vds = V.toDS()
    Vds.createOrReplaceTempView("Vds")
    
    val n = Vds.count
    val size = sizeof(1L, 1.0D)
    println("*** %d  %.2f GB".format(length,length.toDouble*size/(1024.0*1024.0*1024.0)))
    def test () {
      var t: Long = System.currentTimeMillis()

      try {
     	 var sum = V.map{ case (_,v)=> v }.reduce(_+_)
   	 println(sum)
     	 println("**** SparkRDD run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""
         var sum: Double = 0.0;

         for i = 0, n-1 do
           sum += V[i];

         println(sum);
        """)

      println("**** Diablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x); }
      
      t = System.currentTimeMillis()

      try{
       s(sc,"""
        var sum: Double = 0.0;

        for i = 0, n-1 do
       	  sum += Vds[i];

     """)
         println("**** SqlGen run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
	} catch { case x: Throwable => println(x) }

     t = System.currentTimeMillis()
     try{
      var sum = spark.sql("SELECT SUM(Vds._2) FROM Vds")
      sum.createOrReplaceTempView("sum");
      println(sum.show())
      println("**** SparkSQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      }catch { case x: Throwable => println(x) }

    }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}
