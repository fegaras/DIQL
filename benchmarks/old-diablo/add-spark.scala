import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._

object AddSpark {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("AddSpark")
    val sc = new SparkContext(conf)

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    var M = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toInt,a(1).toInt),a(2).toDouble) } )
    var N = sc.textFile(args(1))
              .map( line => { val a = line.split(",")
                              ((a(0).toInt,a(1).toInt),a(2).toDouble) } )

    val t: Long = System.currentTimeMillis()

    val R = M.cogroup(N)
             .map{ case (k,(ms,ns))
                     => ( k, if (ms.isEmpty)
                                ns.head
                             else if (ns.isEmpty)
                                ms.head
                             else ns.head + ms.head ) }

    println(R.count)

    println("**** AddSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    sc.stop()

  }
}
