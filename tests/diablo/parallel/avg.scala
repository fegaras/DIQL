import edu.uta.diql._
import scala.io.Source
import scala.collection.parallel.ParIterable

object Test {

  def main ( args: Array[String] ) {

    explain(true)

    val A = Source.fromFile(args(0)).getLines
                  .zipWithIndex.map{ case (line,i) => (i.toLong,line.toDouble) }
                  .toIterable.par

    v("""

      var sum: Double = 0.0;
      var count: Int = 0;

      for i = 1, 100 do {
          sum += A[i];
          count += 1;
      };
      println(sum/count);

     """)

  }
}
