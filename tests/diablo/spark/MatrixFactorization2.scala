import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import Math._

object Test {

  val a: Double = 0.002
  val b: Double = 0.02

  implicit class Mult ( private val value: Double ) extends AnyVal {
    def ^ ( that: Double ): Double
      = value+(1-a*b)*that
  }

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    val n = args(1).toLong
    val m = args(2).toLong
    val l = args(3).toLong

    var R = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )

    v(sc,"""

      var P: matrix[Double] = matrix();
      var Q: matrix[Double] = matrix();
      var pq: matrix[Double] = matrix();
      var E: matrix[Double] = matrix();

      for i = 0, n-1 do
          for k = 0, l-1 do
              P[i,k] := random();

      for k = 0, l-1 do
          for j = 0, m-1 do
              Q[k,j] := random();

      var steps: Int = 0;
      while ( steps < 10 ) {
        steps += 1;
        for i = 0, n-1 do
            for j = 0, m-1 do {
                pq[i,j] := 0.0;
                for k = 0, l-1 do
                    pq[i,j] += P[i,k]*Q[k,j];
                E[i,j] := R[i,j]-pq[i,j];
                for k = 0, l-1 do {
                    P[i,k] := P[i,k] ^ 2*a*E[i,j]*Q[k,j];
                    Q[k,j] := Q[k,j] ^ 2*a*E[i,j]*P[i,k];
                };
            };
      };

      P.take(30).foreach(println);
      Q.take(30).foreach(println);
    """)

  }
}
