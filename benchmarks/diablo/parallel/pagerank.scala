import edu.uta.diql._
import Math._
import scala.io.Source
import scala.collection.parallel.mutable.ParArray

object Pagerank {

  def main ( args: Array[String] ) {

    //explain(true)

    val E = Source.fromFile(args(0)).getLines
              .map( line => { val a = line.split(",").toList
                              ((a(0).toLong,a(1).toLong),true) } ).toArray.par

    val t: Long = System.currentTimeMillis()

    v("""

      var P: vector[Double] = vector();
      var Q: vector[Double] = vector();
      var C: vector[Int] = vector();
      var N: Int = args(1).toInt;
      var b: Double = 0.85;

      for i = 1, N do {
          C[i] := 0;
          P[i] := 1.0/N;
      };

      for i = 1, N do
          for j = 1, N do
             if (E[i,j])
                C[i] += 1;

      var k: Int = 0;
      while (k < 10) {
        k += 1;
        for i = 1, N do
            Q[i] := P[i];
        for i = 1, N do
            P[i] := 0.0;
        for i = 1, N do
            for j = 1, N do
                if (E[j,i])
                   P[i] += b*Q[j]/C[j];
        for i = 1, N do
            P[i] += (1-b)/N;
      };

      println(P.size);

     """)

    println("**** Pagerank run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }
}
