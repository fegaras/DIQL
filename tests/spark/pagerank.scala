/*
 * Copyright © 2017 University of Texas at Arlington
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    case class GraphNode ( id: Long, rank: Double, adjacent: List[Long] )
    case class PageRank ( id: Long, rank: Double )

    val graph_size = 1000
    val factor = 0.85

    explain(true)

   q("""
      select PageRank( id = x.id, rank = x.rank )
      from x <- ( repeat graph = select GraphNode( id = n.toLong,
                                                   rank = 0.5D,
                                                   adjacent = ns.map(_.toLong) )
                                 from line <- sc.textFile("graph.txt"),
                                      n::ns = line.split(",").toList
                  step select GraphNode( id = m.id, rank = n.rank, adjacent = m.adjacent )
                       from n <- (select PageRank( id = key,
                                                   rank = (1-factor)/graph_size
                                                          +factor*(+/select x.rank from x <- c) )
                                  from c <- ( select PageRank( id = a,
                                                               rank = n.rank/(count/n.adjacent) )
                                              from n <- graph,
                                                   a <- n.adjacent )
                                  group by key: c.id),
                            m <- graph
                       where n.id == m.id
                  limit 10 )
      order by (x.rank) desc
     """).foreach(println)

  }
}
