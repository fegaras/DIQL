import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    val V = sc.textFile(args(0)).map( line => line.toDouble )

    v(sc,"""
      var count1: Int = 0;
      var count2: Int = 0;

      for v in V do {
         if (v==100)
            count1 += 1;
          if (v==110)
            count2 += 1;
      };

     """)
    sc.stop()
  }
}
