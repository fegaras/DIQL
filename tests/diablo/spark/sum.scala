import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    val A = sc.textFile(args(0)).map( line => line.toDouble )

    val N = A.count()

    v(sc,"""
      var sum: Double = 0.0;
      var count: Int = 0;

      for a in A do {
          sum += a;
          count += 1;
      };

      println(sum+" "+count);
     """)

  }
}
