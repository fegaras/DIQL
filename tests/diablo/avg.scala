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
                              ((a(0).toInt,a(1).toInt),a(2).toDouble) } )
    var N = sc.textFile(args(1))
              .map( line => { val a = line.split(",")
                              ((a(0).toInt,a(1).toInt),a(2).toDouble) } )

    v("""

      var V: vector[double];
      var sum: double;
      var count: int;
      var A: double;

      sum = 0.0;
      count = 0;
      for i = 1, 100 do {
          sum = sum + V[i];
          count = count + 1;
      };
      A = sum/toDouble(count);

     """)

  }
}
