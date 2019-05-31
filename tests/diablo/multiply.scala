import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    var M = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )
    var N = sc.textFile(args(1))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )

    v(sc,"""

      var R: matrix[Double] = matrix();

      for i = 0, 199 do
        for j = 0, 299 do {
          R[i,j] := 0.0;
          for k = 0, 199 do
            R[i,j] := R[i,j]+M[i,k]*N[k,j];
        };

      R.foreach(println);

    """)

  }
}
