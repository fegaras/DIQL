import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._

object Test {

  case class Edge ( src: Int, dest: Int )

  case class Rank ( id: Int, degree: Int, rank: Double )

  val alpha = 0.85D

  def main ( args: Array[String] ) {
    val iterations = 10
    val input_file = "g.txt"
    val output_file = ""
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    debug(true)
    val t: Long = System.currentTimeMillis()

   q("""
     let edges = select Edge(s,d)
                 from line <- sc.textFile(input_file),
                      List(s,d) = line.split(",").toList.map(_.toInt)
     in select Rank( id = s, degree = (count/d).toInt, rank = 1-alpha )
        from Edge(s,d) <- edges
        group by s
        order by (count/d) desc
     """).take(10).foreach(println)

    println("DIQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }
}
