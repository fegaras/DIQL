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

   M.map{ case ((i,j),v) => (j,(i,v)) }
    .cogroup( N.map{ case ((i,j),v) => (i,(j,v)) } )
    .flatMap{ case (k,(ms,ns)) => ms.flatMap{ case (i,m) => ns.map{ case (j,n) => ((i,j),m*n) } } }
    .reduceByKey(_+_)
    .repartition(1).sortByKey().foreach(println)

  }
}
