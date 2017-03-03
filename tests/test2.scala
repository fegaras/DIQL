import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)
    val R = sc.textFile("graph.txt")
              .map( line => { val a = line.split(",").toList
                              ("x"+a.head,a.head.toLong)
                            } )
    val S = sc.textFile("graph.txt")
              .map( line => { val a = line.split(",").toList
                              (a.head.toLong,a.head.toLong,a.tail.map(_.toLong))
                            } )

     debug(true)

     case class Person(x:String,y:Long)
    var P: HadoopRDD[Long,Person] = null
     q("""
       min/(select x from (x,_) <- P)
    """)

  }
}
