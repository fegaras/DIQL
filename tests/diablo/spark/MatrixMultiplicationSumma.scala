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

    val R = core.GroupByJoin.groupByJoin[((Int,Int),Double),((Int,Int),Double),Int,Int,Int,Double](
                { case ((i,j),m) => i },
                { case ((i,j),n) => j },
                { case ((_,m),(_,n)) => m*n },
                _+_,
                M.map{ case x@((i,j),m) => (j,x)},
                N.map{ case x@((i,j),b) => (i,x)}
             )
    R.sortBy(_._1,true,1).take(30).foreach(println)

  }
}
