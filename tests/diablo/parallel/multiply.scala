import edu.uta.diql._
import scala.io.Source
import scala.collection.parallel.ParIterable

object Test {

  def main ( args: Array[String] ) {

    explain(true)

    val n = args(2).toLong
    val m = n

    var M = Source.fromFile(args(0)).getLines
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } ).toIterable.par
    var N = Source.fromFile(args(1)).getLines
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } ).toIterable.par

    val x = ""

    v(x,"""

      var R: matrix[Double] = matrix();

      for i = 0, n-1 do
          for j = 0, n-1 do {
               R[i,j] := 0.0;
               for k = 0, m-1 do
                   R[i,j] += M[i,k]*N[k,j];
          };

      R.sortBy(_._1,true,1).take(30).foreach(println);

    """)

  }
}
