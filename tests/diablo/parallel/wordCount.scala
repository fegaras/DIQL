import edu.uta.diql._
import scala.io.Source
import scala.collection.parallel.ParIterable

object Test {

  def main ( args: Array[String] ) {

    explain(true)

    val words = Source.fromFile(args(0)).getLines
                  .flatMap( line => line.split(" ") ).toIterable.par

    val x = ""

    v(x,"""

      var C: map[String,Int] = map();

      for w in words do
          C[w] := 0;

      for w in words do
          C[w] += 1;

      C.foreach(println);

     """)

  }
}
