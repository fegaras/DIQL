import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {

    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    val P = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              (a(0).toDouble, a(1).toDouble) } )

    var N = P.count()

    v(sc,"""

      var sum_x: Double = 0.0;
      var sum_y: Double = 0.0;
      var x_bar: Double = 0.0;
      var y_bar: Double = 0.0;
      var xx_bar: Double = 0.0;
      var yy_bar: Double = 0.0;
      var xy_bar: Double = 0.0;
      var slope: Double = 0.0;
      var intercept: Double = 0.0;

      for p in P do {
          sum_x += p._1;
          sum_y += p._2;
      };

      x_bar := sum_x/N;
      y_bar := sum_y/N;

      for p in P do {
          xx_bar += (p._1 - x_bar)*(p._1 - x_bar);
          yy_bar += (p._2 - y_bar)*(p._2 - y_bar);
          xy_bar += (p._1 - x_bar)*(p._2 - y_bar);
     };
     
     slope := xy_bar/xx_bar;
     intercept := y_bar - slope*x_bar;

     println(slope); 
     println(intercept);

     """)

  }
}
