import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    var M = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toInt,a(1).toInt),a(2).toDouble) } )
    var N = sc.textFile(args(1))
              .map( line => { val a = line.split(",")
                              ((a(0).toInt,a(1).toInt),a(2).toDouble) } )

    M.map{ case ((i,j),m) => (j,(i,m)) }
     .join( N.map{ case ((i,j),n) => (i,(j,n)) } )
     .map{ case (k,((i,m),(j,n))) => ((i,j),m*n) }
     .reduceByKey(_+_)
     .sortBy(_._1,true,1).take(30).foreach(println)

  }
}
