import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util._

object Test {

  def main ( args: Array[String] ): Unit = {
    val conf = new SparkConf().setAppName("NestedData")
    val sc = new SparkContext(conf)
    val n = Integer.parseInt(args(0))
    val CF = args(1)
    val OF = args(2)
    val rand = new Random()
    def randomValue ( n: Int ): Int = Math.round(rand.nextDouble()*n).toInt
    sc.parallelize(1 to n).map( _ => (randomValue(n),rand.nextDouble()*1000.0) )
      .map{ case (a,b) => "\"XYZ\","+a+","+b }.repartition(1).saveAsTextFile(CF)
    sc.parallelize(1 to 10*n).map( _ => (randomValue(10*n),randomValue(n),rand.nextDouble()*100.0) )
      .map{ case (a,b,c) => a+","+b+","+c }.repartition(1).saveAsTextFile(OF)
  }
}
