import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._

object Test {

  case class Customer ( name: String, cid: Int, account: Float )

  case class Order ( oid: Int, cid: Int, price: Float )

  def main ( args: Array[String] ) {
    val CF = args(0)
    val OF = args(1)
    val output_file = args(2)
    val conf = new SparkConf().setAppName("Nested")
    val sc = new SparkContext(conf)

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    explain(true)
    val t: Long = System.currentTimeMillis()

    val customers = sc.textFile(CF).map{ line => line.split(",")
                          match { case Array(a,b,c) => Customer(a,b.toInt,c.toFloat) } }
    val orders = sc.textFile(OF).map{ line => line.split(",")
                          match { case Array(a,b,c) => Order(a.toInt,b.toInt,c.toFloat) } }
/*
    q("""
     select c.name
     from c <- customers
     where c.account < +/(select o.price from o <- orders where o.cid == c.cid)
        && 100 > avg/(select o.price from o <- orders where o.cid == c.cid)
     """).saveAsTextFile(output_file)
     *
     *      where c.account < +/(select o.price from o <- orders where o.cid == c.cid
                              && count/(select d from d <- customers where o.cid == d.cid) > 100)
*/
    q("""
     select ( k, +/c.account )
     from c <- customers
     where c.account < 100.0
     group by k: c.cid
     """).saveAsTextFile(output_file)

    sc.stop()

    println("**** DIQL Spark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }
}
