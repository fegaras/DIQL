import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random

object PCA{

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val nn = args(1).toInt
    val mm = args(2).toInt
     
    val conf = new SparkConf().setAppName("Multiply")
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

    def randomMatrix ( n: Int, m: Int ) = {
      val max = 10
      val l = Random.shuffle((0 until n-1).toList)
      val r = Random.shuffle((0 until m-1).toList)
      sc.parallelize(l)
        .flatMap{ i => r.map{ j => ((i.toLong,j.toLong),rand.nextDouble()*max) } }
        .cache()
    }

    val P = randomMatrix(nn,mm)
    val Pds = P.toDS()
    val n = Pds.count()
    val d = mm-1
    val r:Double = n/d
    val m = r.toLong
    val x = m+1
    val z = m-1
    Pds.createOrReplaceTempView("Pds")
    val size = sizeof(((1L,1L),1.0D))
    println("*** %d %d  %.2f GB".format(nn,mm,(nn.toDouble*mm)*size/(1024.0*1024.0*1024.0)))

    def test () {
      var t: Long = System.currentTimeMillis()

      try { 
	val mean = P.map(x => (x._1._2, x._2)).reduceByKey(_+_).map(x => (x._1, x._2/r)).sortByKey().collect()

   	def v(i: Double, value:Double) =
   	   value - mean(i.toInt)._2
 
	
	val cov = P.map{ case ((i,j),v) => (i,(j,v)) }
     	 .cogroup( P.map{ case ((i,j),v) => (i,(j,v)) } )
       	 .flatMap{case (k,(ms,ns)) => ms.flatMap{ case (i,m) => ns.map{
          case (j,n) => ((i,j), v(i,m)*v(j,n))
       	 }}}
     	 .reduceByKey(_+_)
     	 .map{ case ((i,j),v) => (i,j,v/z) }
 
        println(cov.count)
      println("**** SparkRDD run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""
        var mean: vector[Double] = vector();
        var sum: matrix[Double] = matrix();

        for i = 0, n-1 do {
          for j = 0, x-1 do
            mean[j] += P[i,j];
         };

         for i = 0, n-1 do {
           mean[i] := 0.0+mean[i]/r;
         };

         for i = 0, d-1 do {
           for j = 0, d-1 do {
             sum[i,j] := 0.0;
             for k = 0, x-1 do
               sum[i,j] += (P[k,i]-mean[i])*(P[k,j]-mean[j]);
           };
         }; 

          for i = 0, d-1 do {
            for j = 0, d-1 do {
              sum[i,j] := 0.0 + sum[i,j]/z;
            };
          };

        println(sum.count);

        """)

      println("**** Diablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
   
      t = System.currentTimeMillis()

      try {
      s(sc,"""
        var mean: vector[Double] = vector();
        var sum: matrix[Double] = matrix();

        for i = 0, n-1 do {
          for j = 0, x-1 do
            mean[j] += Pds[i,j];
         };

         for i = 0, n-1 do {
           mean[i] := 0.0+mean[i]/r;
         };

         for i = 0, d-1 do {
           for j = 0, d-1 do {
             sum[i,j] := 0.0;
             for k = 0, x-1 do
               sum[i,j] += (Pds[k,i]-mean[i])*(Pds[k,j]-mean[j]);
           };
         };

          for i = 0, d-1 do {
            for j = 0, d-1 do {
              sum[i,j] := 0.0 + sum[i,j]/z;
            };
          };

        println(sum.count);
        """)

      println("**** SQLGen run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()
      try{
         var mean = spark.sql("SELECT Pds._1._2 AS _1, SUM(Pds._2) AS _2 FROM Pds GROUP BY Pds._1._2");
         mean.createOrReplaceTempView("mean");
  
 	 mean = spark.sql(StringContext("SELECT mean._1 AS _1 , mean._2 / ", " AS _2  FROM mean").s(r));
 	 mean.createOrReplaceTempView("mean");
  
 	 var sum = spark.sql("SELECT Pds._1._2 AS _1_1,  P2ds._1._2 AS _1_2, SUM((Pds._2 - mean._2) * (P2ds._2 - mean2._2)) AS _2 FROM mean mean2 JOIN Pds P2ds ON mean2._1 == P2ds._1._2  JOIN Pds ON P2ds._1._1 == Pds._1._1 JOIN mean ON mean._1 == Pds._1._2  GROUP BY Pds._1._2, P2ds._1._2");
 	 sum.createOrReplaceTempView("sum");
  
 	 sum = spark.sql(StringContext("SELECT sum._1_1, sum._1_2, sum._2 / ", " FROM sum").s(z));
  	sum.createOrReplaceTempView("sum");
 	println(sum.count)

       println("**** SparkSQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      }catch { case x: Throwable => println(x) }
 }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}

