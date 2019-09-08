import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object StringMatchSpark {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("StringMatch").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val w = sc.textFile("w.txt")
              .map{ case (strings) => ((strings), true) }

    val k = sc.textFile("k.txt")
      .map{ case (strings) => ((strings), false) }


    val res = k.join(w)
      .map { case (key, (_, r)) => (key, r) }


    res.foreach(println)

    sc.stop()

  }
}