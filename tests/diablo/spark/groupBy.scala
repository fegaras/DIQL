import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

case class R ( K: Long, A: Double )

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    val V = sc.textFile(args(0)).map {
               case line
                 => val a = line.split(",")
                    R(a(0).toLong, a(1).toDouble)
               }

    v(sc,"""

      var C: vector[Double] = vector();

      for v in V do {
	  C[v.K] += v.A;

      C.foreach(println);

      };
     
     """)

  }
}
