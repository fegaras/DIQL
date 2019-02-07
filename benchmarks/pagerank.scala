import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._

object Test {

  case class Edge ( src: Int, dest: Int )

  case class Rank ( id: Int, degree: Int, rank: Double )

  val alpha = 0.85D

  def main ( args: Array[String] ) {
    val iterations = args(0).toInt
    val input_file = args(1)
    val output_file = args(2)
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    explain(true)
    val t: Long = System.currentTimeMillis()

   q("""
     let edges = select Edge(s,d)
                 from line <- sc.textFile(input_file),
                      List(s,d) = line.split(",").toList.map(_.toInt)
     in repeat nodes = select Rank( id = s,
                                    degree = (count/d).toInt,
                                    rank = 1-alpha )
                       from Edge(s,d) <- edges
                       group by s
        step select Rank( id, m.degree, nr )
             from (id,nr) <- ( select ( key,
                                        (1-alpha)+alpha*(+/rank)/(+/degree) )
                               from Rank(id,degree,rank) <- nodes,
                                    e <- edges
                               where e.src == id
                               group by key: e.dest ),
                  m <- nodes
             where id == m.id
        limit iterations
     """).saveAsTextFile(output_file)

    sc.stop()

    println("**** DIQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }
}
