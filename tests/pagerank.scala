import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    case class GraphNode ( id: Long, rank: Double, adjacent: List[Long] )
    case class PageRank ( id: Long, rank: Double )

    var graph = sc.textFile("graph.txt")
                  .map( line => { val a = line.split(",").toList
                                  GraphNode(id = a.head.toLong,
                                            rank = 0.5D,
                                            adjacent = a.tail.map(_.toLong))
                                } )

    val graph_size = graph.count()

    // damping factor
    val factor = 0.85

    debug(true)

    for ( i <- Range(0,10) )
       graph = q("""
         select GraphNode( id = m.id, rank = n.rank, adjacent = m.adjacent )
         from n in (select PageRank( id = key,
                                     rank = (1-factor)/graph_size+factor*(+/select x.rank from x in c) )
                    from c in ( select PageRank( id = a, rank = n.rank/(count/n.adjacent) )
                                from n <- graph,
                                     a <- n.adjacent )
                    group by key: c.id),
              m <- graph
         where n.id == m.id
       """).cache()

      q("""
        select PageRank( id = x.id, rank = x.rank )
        from x <- graph
        order by desc x.rank
        """).foreach(println)

  }
}
