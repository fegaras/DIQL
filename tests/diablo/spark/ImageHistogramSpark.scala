import org.apache.spark.{SparkConf, SparkContext}

case class Color ( red: Int, green: Int, blue: Int )

object Test {

  def main ( args: Array[String] ) {

    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    var P = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              Color(a(0).toInt,a(1).toInt,a(2).toInt) } )

    val R = P.map(_.red).countByValue()
    val G = P.map(_.green).countByValue()
    val B = P.map(_.blue).countByValue()

    println(R)
    println(G)
    println(B)
  }
}
