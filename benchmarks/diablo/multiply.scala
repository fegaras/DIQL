import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random


object Multiply {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val n = args(1).toInt
    val m = args(2).toInt
    val mm = m

    val conf = new SparkConf().setAppName("Multiply")
    val sc = new SparkContext(conf)
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

    def test () {
      var t: Long = System.currentTimeMillis()

      try {
      val R = core.GroupByJoin.groupByJoin[((Long,Long),Double),((Long,Long),Double),Long,Long,Long,Double](
                    { case ((i,j),m) => i },
                    { case ((i,j),n) => j },
                    { case ((_,m),(_,n)) => m*n },
                    _+_,
                    M.map{ case x@((i,j),m) => (j,x)},
                    N.map{ case x@((i,j),b) => (i,x)}
             )

      println(R.count)

      println("**** MultiplySpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""

         var R: matrix[Double] = matrix();

         for i = 0, n-1 do
             for j = 0, n-1 do {
                  R[i,j] := 0.0;
                  for k = 0, mm-1 do
                      R[i,j] += M[i,k]*N[k,j];
             };

         println(R.count);

        """)

      println("**** MultiplyDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
    }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}
