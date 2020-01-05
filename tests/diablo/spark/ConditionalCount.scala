import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._


object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    var V = sc.textFile(args(0)).map(_.split(",")).map(_(0).toDouble)

    v(sc,"""
      var count: Int = 0;

      for v in V do
          if (v < 100)
             count += 1;

      println(count);
     """)

  }
}
