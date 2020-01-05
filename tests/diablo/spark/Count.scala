import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    var V = sc.textFile(args(0))

    v(sc,"""
      var count: Int = 0;

      for v in V do
          count += 1;

      println(count);
     """)

  }
}
