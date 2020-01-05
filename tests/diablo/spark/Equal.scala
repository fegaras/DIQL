import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    var V = sc.textFile(args(0)).flatMap(_.split(","))

    var x = V.first()

    v(sc,"""
      var eq: Boolean = true;

      for v in V do
          eq := eq && v == x;

      println(eq);

     """)

  }
}
