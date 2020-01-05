import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Test {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("StringMatch")
    val sc = new SparkContext(conf)

    val words = sc.textFile(args(0))
                  .flatMap( line => line.split(" ") )

    val keys = sc.textFile(args(1)).collect()

    val res = words.filter{ w => keys.contains(w) }.distinct()

    res.foreach(println)

    sc.stop()

  }
}
