import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._

object Multiply {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Multiply")
    val sc = new SparkContext(conf)

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    //explain(true)

    val n = args(2).toLong
    val m = n

    var M = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )
    var N = sc.textFile(args(1))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )

    val t: Long = System.currentTimeMillis()

    v(sc,"""

      var R: matrix[Double] = matrix();

      for i = 0, n-1 do
          for j = 0, n-1 do {
               R[i,j] := 0.0;
               for k = 0, m-1 do
                   R[i,j] += M[i,k]*N[k,j];
          };

      println(R.count);

    """)

    println("**** Multiply run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    sc.stop()

  }
}
