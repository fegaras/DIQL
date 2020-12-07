import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.util.Random

case class Color ( red: Int, green: Int, blue: Int )

object Histogram {

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
    def byte () = Math.abs(rand.nextInt()) % 256
    
    val P = sc.parallelize(1L to length/100)
              .flatMap{ i => (1 to 100).map{ j => Color(byte(),byte(),byte()) } }
              .zipWithIndex.map{case (line,i) => (i, line)}    
              .cache()

    val size = sizeof(1L, Color(1,1,1))
    println("*** %d  %.2f GB".format(length,length.toDouble*size/(1024.0*1024.0*1024.0)))


    val Pds = P.toDS()
    Pds.createOrReplaceTempView("Pds")

    val n = Pds.count()

    def test () {
      var t: Long = System.currentTimeMillis()

      try {
   	 val R = P.map{case (_,(Color(red, _, _)))=> red}.countByValue()
   	 val G = P.map{case (_,(Color(_, green, _)))=> green}.countByValue()
   	 val B = P.map{case (_,(Color(_, _, blue)))=> blue}.countByValue()

         println(R.size)
     	 println(G.size)
     	 println(B.size)

      println("**** SparkRDD run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""

         var R: map[Int,Int] = map();
         var G: map[Int,Int] = map();
         var B: map[Int,Int] = map();

         for i = 0, n-1 do {
             R[P[i].red] += 1;
             G[P[i].green] += 1;
             B[P[i].blue] += 1;
         };

         println(R.count);
         println(G.count);
         println(B.count);
     
        """)

      println("**** Diablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
    
    
    t = System.currentTimeMillis()

      try{
       s(sc,"""
       	 var R: map[Int,Int] = map();
         var G: map[Int,Int] = map();
         var B: map[Int,Int] = map();
         
         for i = 0, n-1 do {
             R[Pds[i].red] += 1;
             G[Pds[i].green] += 1;
             B[Pds[i].blue] += 1;
         };  
         
         println(R.count);
         println(G.count);
         println(B.count);
     """)
         println("**** SqlGen run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
	} catch { case x: Throwable => println(x) }

    t = System.currentTimeMillis()

      try{
      	 var R = spark.sql("SELECT Pds._2.red AS _1, COUNT(Pds._1) AS _2 FROM Pds GROUP BY Pds._2.red");
      	 R.createOrReplaceTempView("R");
      	 println(R.count);
 	 var G = spark.sql("SELECT Pds._2.green AS _1, COUNT(Pds._1) AS _2 FROM Pds GROUP BY Pds._2.green");
 	 G.createOrReplaceTempView("G");
 	 println(G.count);
 	 var B = spark.sql("SELECT Pds._2.blue AS _1, COUNT(Pds._1) AS _2 FROM Pds GROUP BY Pds._2.blue");
 	 B.createOrReplaceTempView("B");
 	 println(B.count);
       println("**** SparkSql run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
}
    for ( i <- 1 to repeats )
        test()
    sc.stop()
  }
}
