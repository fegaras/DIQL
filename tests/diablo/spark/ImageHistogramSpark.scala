import org.apache.spark.{SparkConf, SparkContext}

object ImageHistogramSpark {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    var P = sc.textFile(args(0))
      .map(line => {
        val a = line.split(",")
        (a(0).toInt, a(1).toInt, a(2).toInt)
      })


    val R = P.map(r => r._1).countByValue()
    val G = P.map(g => g._1).countByValue()
    val B = P.map(b => b._1).countByValue()

    println(R)
    println(G)
    println(B)
  }
}
