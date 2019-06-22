import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._

object Add {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Add")
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

      for i = 0, 100 do
        for j = 0, 100 do {
          if (!exists(M[i,j]))
            R[i,j] := N[i,j];
          else if (!exists(N[i,j]))
            R[i,j] := M[i,j];
          else R[i,j] := M[i,j]+N[i,j];
        };

      println(R.count());

    """)

    println("**** Add run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    sc.stop()

  }
}
