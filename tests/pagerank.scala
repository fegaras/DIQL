import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object PageRank {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    case class Node ( id: Long, rank: Double, adjacent: List[Long] )
    case class PageRank ( id: Long, rank: Double )

    val graph = sc.textFile("graph.txt")
                  .map( line => { val a = line.split(",").toList
                                  Node(id = a.head.toLong,
                                       rank = 0.5D,
                                       adjacent = a.tail.map(_.toLong))
                                } )

    val graph_size = graph.count()

    // damping factor
    val factor = 0.85

    debug(true)

    q("""
      select PageRank( id = key,
                                  rank = (1-factor)/graph_size+factor*(+/select x.rank from x in c) )
                 from c in ( select PageRank( id = a, rank = n.rank/(count/n.adjacent) )
                             from n in graph, a in n.adjacent )
                 group by key: c.id
      """)
    
    q("""
      select Node( id = m.id, rank = n.rank, adjacent = m.adjacent )
      from n in (select PageRank( id = key,
                                  rank = (1-factor)/graph_size+factor*(+/select x.rank from x in c) )
                 from c in ( select PageRank( id = a, rank = n.rank/(count/n.adjacent) )
                             from n in graph, a in n.adjacent )
                 group by key: c.id),
           m in graph
      where n.id == m.id
      """)

/*                   
select < node: x.id, rank: x.rank >
from x in (repeat nodes = select < id: key, rank: 1.0/graph_size as double, adjacent: al >
                            from (key,al) in graph
             step select (< id: m.id, rank: n.rank, adjacent: m.adjacent >,
                          abs((n.rank-m.rank)/m.rank) > 0.1)
                    from n in (select < id: key,
                                        rank: (1-factor)/graph_size+factor*sum(select x.rank from x in c) >
                                 from c in ( select < id: a, rank: n.rank/count(n.adjacent) >
                                               from n in nodes, a in n.adjacent )
                                group by key: c.id),
                         m in nodes
                   where n.id = m.id
            limit 10)
order by x.rank desc
""").foreach(println)
*/

  }
}
