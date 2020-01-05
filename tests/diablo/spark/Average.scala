import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)


    val A = sc.textFile(args(0)).zipWithIndex.map{ case (line,i) => (i,line.toDouble) }

    v(sc,"""

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
