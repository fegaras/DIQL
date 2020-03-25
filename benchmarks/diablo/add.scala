import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import scala.util.Random


object Add {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val n = args(1).toInt
    val m = args(2).toInt
    val mm = m

    val conf = new SparkConf().setAppName("Add")
    val sc = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    def randomMatrix ( n: Int, m: Int ) = {
      val max = 10
      val l = Random.shuffle((0 until n).toList)
      val r = Random.shuffle((0 until m).toList)
      sc.parallelize(l)
        .flatMap{ i => val rand = new Random()
                       r.map{ j => ((i.toLong,j.toLong),rand.nextDouble()*max) } }
        .cache()
    }

    val M = randomMatrix(n,m)
    val N = randomMatrix(n,m)

    val size = sizeof(((1L,1L),1.0D))

    println("*** %d %d  %.2f GB".format(n,m,(n.toDouble*m)*size/(1024.0*1024.0*1024.0)))

    def test () {
      var t: Long = System.currentTimeMillis()

      try {
      val R = M.join(N).mapValues{ case (m,n) => n + m }

      println(R.count)

      println("**** AddSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""

         var R: matrix[Double] = matrix();

         for i = 0, n-1 do
           for j = 0, mm-1 do
               R[i,j] := M[i,j]+N[i,j];

         println(R.count());

        """)

      println("**** AddDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
    }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}
