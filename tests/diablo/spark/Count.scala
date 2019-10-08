import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    var V = sc.textFile(args(0))
      .zipWithIndex.map{ case (line,i)
    => { val a = line.split(",")
      (i.toLong,(a(0).toDouble)) } }

    var N = V.count()

    println(N);

    v(sc,"""
      var count: Int = 0;

      for i = 0, N-1 do {
          count += 1;
      };

     """)
    sc.stop()
  }
}
