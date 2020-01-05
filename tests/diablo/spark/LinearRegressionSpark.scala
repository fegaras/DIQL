import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("LinearRegression")
    val sc = new SparkContext(conf)

    val p = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              (a(0).toDouble, a(1).toDouble) } )

    val n = p.count()
    val x_bar = p.map(_._1).reduce(_+_)/n
    val y_bar = p.map(_._2).reduce(_+_)/n

    val xx_bar = p.map(x => (x._1 - x_bar)*(x._1 - x_bar)).reduce(_+_)
    val yy_bar = p.map(y => (y._2 - y_bar)*(y._2 - y_bar)).reduce(_+_)
    val xy_bar = p.map(p => (p._1 - x_bar)*(p._2 - y_bar)).reduce(_+_)

    val slope = xy_bar/xx_bar
    val intercept = y_bar - slope * x_bar

    println(slope)
    println(intercept)

  }
}
