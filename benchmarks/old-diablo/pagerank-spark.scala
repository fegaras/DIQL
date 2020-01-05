import org.apache.spark._
import org.apache.spark.rdd._
import Math._
import org.apache.log4j._

object PagerankSpark {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("PagerankSpark")
    val sc = new SparkContext(conf)

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val E = sc.textFile(args(0))
              .map( line => { val a = line.split(",").toList
                              (a(0).toLong,a(1).toLong) } )

    val N = args(1).toInt

    val t: Long = System.currentTimeMillis()

    val links = E.groupByKey().cache()
    var ranks = links.mapValues(v => 1.0/N)

    for (i <- 1 to 10) {
        val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
             val size = urls.size
             urls.map(url => (url, rank / size))
        }
        ranks = contribs.reduceByKey(_ + _).mapValues(0.15/N + 0.85 * _)
    }
    println(ranks.count)

    println("**** PagerankSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    sc.stop()

  }
}
