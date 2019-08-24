import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    val n = args(2).toLong
    val m = n

    var M = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )
    var N = sc.textFile(args(1))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )


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

      R.take(30).foreach(println);
      println(R.count());

    """)

  }
}
