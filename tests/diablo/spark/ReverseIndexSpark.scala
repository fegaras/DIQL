import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main ( args: Array[String] ) {

    val conf = new SparkConf().setAppName("reverse-index")
    val sc = new SparkContext(conf)

    val regex = "<a\\s+(?:[^>]*?\\s+)?href=([\"'])(.*?)\\1".r

    var ri = sc.wholeTextFiles("reverseIndex/")
               .map{ case (k, v) => (k, regex.findAllMatchIn(v).toList) }
               .flatMap{ case (k, v) => v.map(s => (s.toString, k)) }
               .reduceByKey{ case (x, y) => x +","+  y }

    ri.foreach(println)
  }
}
