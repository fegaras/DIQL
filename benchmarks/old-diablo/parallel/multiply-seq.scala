import edu.uta.diql._
import scala.io.Source

object Multiply {

  def main ( args: Array[String] ) {

    //explain(true)

    val n = args(2).toLong
    val m = n

    val M = Source.fromFile(args(0)).getLines.toIterable
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )

    val N = Source.fromFile(args(1)).getLines.toIterable
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )

    val t: Long = System.currentTimeMillis()

    v("""

      var R: matrix[Double] = matrix();

      for i = 0, n-1 do
          for j = 0, n-1 do {
               R[i,j] := 0.0;
               for k = 0, m-1 do
                   R[i,j] += M[i,k]*N[k,j];
          };

      println(R.size);

    """)

    println("**** Multiply run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }
}
