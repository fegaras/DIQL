import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

      val p = sc.textFile(args(0))
      .map(line => {
        val a = line.split(",")
        (a(0).toDouble, a(1).toDouble)
      }).cache()

     val x:RDD[(Long,Double)] = p.zipWithIndex.map{ case (line,i) =>
     (i.toLong,(line._1)) }

    val y = p.zipWithIndex.map{ case (line,i) =>
     (i.toLong,(line._2)) }

    var N = x.count()

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

      for i = 0, N-1 do {
          sum_x += x[i];
          sum_y += y[i];
      };
     
     x_bar := sum_x/N;
     y_bar := sum_y/N;
      
     for i = 0, N-1 do {
           xx_bar += (x[i] - x_bar)*(x[i] - x_bar);
           yy_bar += (y[i] - y_bar)*(y[i] - y_bar);
           xy_bar += (x[i] - x_bar)*(y[i] - y_bar);
     };
     
     slope := xy_bar/xx_bar;
     intercept := y_bar - slope*x_bar;

     println(slope); 
     println(intercept);
     """)

  }
}

