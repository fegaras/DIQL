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


    v("""

      var R: matrix[Double] = matrix();

      for i = 0, 100 do
        for j = 0, 100 do {
          if (!exists(M[i,j]))
            R[i,j] := N[i,j];
          else if (!exists(N[i,j]))
            R[i,j] := M[i,j];
          else R[i,j] := M[i,j]+N[i,j];
        };

      R.take(30).foreach(println);
      println(R.count());

    """)

  }
}
