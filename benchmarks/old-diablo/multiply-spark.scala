import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._

object MultiplySpark {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("MultiplySpark")
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

    val R = M.map{ case ((i,j),v) => (j,(i,v)) }
             .cogroup( N.map{ case ((i,j),v) => (i,(j,v)) } )
             .flatMap{ case (k,(ms,ns)) => ms.flatMap{ case (i,m) => ns.map{ case (j,n) => ((i,j),m*n) } } }
             .reduceByKey(_+_)

    println(R.count)

    println("**** MultiplySpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    sc.stop()

  }
}
