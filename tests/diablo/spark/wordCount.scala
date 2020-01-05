import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import Math._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    val words = sc.textFile(args(0))
                  .flatMap( line => line.split(" ") )

    v(sc,"""

      var C: map[String,Int] = map();

      for w in words do
          C[w] += 1;

      C.foreach(println);

     """)

  }
}
