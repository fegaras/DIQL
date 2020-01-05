import edu.uta.diql._
import Math._
import scala.io.Source

object Factorization {

  def main ( args: Array[String] ) {

    //explain(true)

    val n = args(1).toLong
    val m = args(2).toLong
    val l = args(3).toLong

    var R = Source.fromFile(args(0)).getLines.toIterable
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )

    val t: Long = System.currentTimeMillis()

    v("""

      var P: matrix[Double] = matrix();
      var Q: matrix[Double] = matrix();
      var pq: matrix[Double] = matrix();
      var E: matrix[Double] = matrix();

      var a: Double = 0.002;
      var b: Double = 0.02;

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
                    P[i,k] += a*(2*E[i,j]*Q[k,j]-b*P[i,k]);
                    Q[k,j] += a*(2*E[i,j]*P[i,k]-b*Q[k,j]);
                };
            };
      };

      println(P.size);
      println(Q.size);

    """)

    println("**** Factorization run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }
}
