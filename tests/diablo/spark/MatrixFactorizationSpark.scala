import org.apache.spark._
import org.apache.spark.rdd._
import Math._
import scala.util._
import org.apache.log4j._

object Test {

  def map ( f: Double => Double, x: RDD[((Long,Long),Double)] ): RDD[((Long,Long),Double)]
    = x.map{ case (k,v) => (k,f(v)) }

  def transpose ( x: RDD[((Long,Long),Double)] ): RDD[((Long,Long),Double)]
    = x.map{ case ((i,j),v) => ((j,i),v) }

  def op ( f: (Double,Double) => Double,
           x: RDD[((Long,Long),Double)],
           y: RDD[((Long,Long),Double)] ): RDD[((Long,Long),Double)]
    = x.cogroup(y)
       .map{ case (k,(ms,ns))
               => ( k, if (ms.isEmpty)
                          f(0.0,ns.head)
                       else if (ns.isEmpty)
                          f(ms.head,0.0)
                       else f(ns.head,ms.head) ) }

  def multiply ( x: RDD[((Long,Long),Double)],
                 y: RDD[((Long,Long),Double)] ): RDD[((Long,Long),Double)]
    = x.map{ case ((i,j),v) => (j,(i,v)) }
       .cogroup( y.map{ case ((i,j),v) => (i,(j,v)) } )
       .flatMap{ case (k,(ms,ns)) => ms.flatMap{ case (i,m) => ns.map{ case (j,n) => ((i,j),m*n) } } }
       .reduceByKey(_+_)

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    val n = args(1).toInt
    val m = args(2).toInt
    val l = args(3).toInt

    val a = 0.002
    val b = 0.02

    var R = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )

    val t: Long = System.currentTimeMillis()

    var P = sc.parallelize((0 to n-1).flatMap( i => (0 to l-1).map{ case j => ((i.toLong,j.toLong),random()) } ))
    var Q = sc.parallelize((0 to l-1).flatMap( i => (0 to m-1).map{ case j => ((i.toLong,j.toLong),random()) } ))

    for ( i <- 1 to 10 ) {
      val E = R.cogroup( multiply(P,Q) )
               .flatMap{ case (k,(ms,ns)) => if (!ms.isEmpty && !ns.isEmpty) List((k,ns.head-ms.head)) else Nil }.cache()
      P = op ( _+_, P, map( _*a, op ( _-_, map( _*2, multiply(E,transpose(Q)) ),
                                           map( _*b, P ) ) ) ).cache()
      Q = op ( _+_, Q, map( _*a, op ( _-_, map( _*2, transpose(multiply(transpose(E),P)) ),
                                           map( _*b, Q ) ) ) ).cache()
    }

    P.collect().foreach(println)
    Q.collect().foreach(println)
    multiply(P,Q).collect().foreach(println)

  }
}
