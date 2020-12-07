import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random

object MatrixAddition {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val n = args(1).toInt
    val m = args(2).toInt
    val mm = m
    val x = m

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

    val M = randomMatrix(n,m)
    val N = randomMatrix(n,m)

    val size = sizeof(((1L,1L),1.0D))
    println("*** %d %d  %.2f GB".format(n,m,(n.toDouble*m)*size/(1024.0*1024.0*1024.0)))

    val Mds = M.toDS()
    val Nds = N.toDS()

    Mds.createOrReplaceTempView("Mds")
    Nds.createOrReplaceTempView("Nds")

    def test () {
      var t: Long = System.currentTimeMillis()

      try {
        val R = M.join(N).mapValues{ case (m,n) => n + m } 
        println(R.count)

      println("**** SparkRDD run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""
        var R: matrix[Double] = matrix();
        for i = 0, n-1 do
          for j = 0, n-1 do
            R[i,j] := M[i,j] + N[i,j];  

        println(R.count);

        """)

      println("**** Diablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
   
      t = System.currentTimeMillis()

      try {
      s(sc,"""
        var R: matrix[Double] = matrix();
        for i = 0, n-1 do
          for j = 0, n-1 do
              R[i,j] := Mds[i,j] + Nds[i,j];  
       
        println(R.count);
        """)

      println("**** SQLGen run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()
      try{
      var R = spark.sql("SELECT Mds._1._1, Mds._1._2, Mds._2 + Nds._2 FROM Nds JOIN Mds ON Nds._1._2 == Mds._1._2  AND Nds._1._1 == Mds._1._1")
      R.createOrReplaceTempView("R");
      println(R.count)
      println("**** SparkSQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      }catch { case x: Throwable => println(x) }
 }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}

