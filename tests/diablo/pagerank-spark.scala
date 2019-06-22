import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import Math._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    val E = sc.textFile(args(0))
              .map( line => { val a = line.split(",").toList
                              (a(0).toLong,a(1).toLong) } )

    val N = args(1).toInt

    val links = E.groupByKey().cache()

    println(links.count)

    var ranks = links.mapValues(v => 1.0/N)

    for (i <- 1 to 10) {
        val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
             val size = urls.size
             urls.map(url => (url, rank / size))
        }
        ranks = contribs.reduceByKey(_ + _).mapValues(0.15/N + 0.85 * _)
    }
    ranks.take(30).foreach(println);
  }
}
