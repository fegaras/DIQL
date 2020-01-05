import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main ( args: Array[String] ) {

    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    val words = sc.textFile(args(0))
                  .flatMap( line => line.split(" ") )

    val counts = words.map((_,1)).reduceByKey(_+_)

    counts.foreach(println)

  }
}
