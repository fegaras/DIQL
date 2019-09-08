import org.apache.spark.{SparkConf, SparkContext}

object WordCountSpark {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val count = sc.textFile(args(0))
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey{case (x, y) => x + y}
    count.foreach(println)

  }
}
