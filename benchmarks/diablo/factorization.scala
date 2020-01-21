import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random
import java.io._


object Factorization {
  val a = 0.002
  val b = 0.02

  implicit class Mult ( private val value: Double ) extends AnyVal {
    def ^ ( that: Double ): Double
      = value+(1-a*b)*that
  }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val n = args(1).toInt
    val m = args(2).toInt
    val d = 2
    val mm = m
    val num_steps = 1

    val conf = new SparkConf().setAppName("Factorization")
    val sc = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    import Math._
    val rand = new Random()
    val l = Random.shuffle((0 until n-1).toList)
    val r = Random.shuffle((0 until m-1).toList)

    val R = sc.parallelize(l)
              .flatMap{ i => r.map{ j => ((i.toLong,j.toLong),Math.floor(rand.nextDouble()*5+1).toInt) } }
              .cache()

    val size = sizeof(((1L,1L),1))
    println("*** %d %d  %.2f GB".format(n,m,(n+m)*size/(1024.0*1024.0*1024.0)))

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
      = x.map{ case ((i,j),m) => (j,(i,m)) }
         .join( y.map{ case ((i,j),n) => (i,(j,n)) } )
         .map{ case (k,((i,m),(j,n))) => ((i,j),m*n) }
         .reduceByKey(_+_)

    def test () {
      var P = sc.parallelize((0 to n-1).flatMap( i => (0 to d-1).map {
                    case j => ((i.toLong,j.toLong),random()) } ))
      var Q = sc.parallelize((0 to d-1).flatMap( i => (0 to m-1).map {
                    case j => ((i.toLong,j.toLong),random()) } ))

      var t: Long = System.currentTimeMillis()

      for ( i <- 1 to num_steps ) {
        val E = R.cogroup( multiply(P,Q) )
                 .flatMap{ case (k,(ms,ns)) => if (!ms.isEmpty && !ns.isEmpty)
                                                  List((k,ns.head-ms.head))
                                               else Nil }.cache()
        P = op ( _+_, P, map( _*a, op ( _-_, map( _*2, multiply(E,transpose(Q)) ),
                                             map( _*b, P ) ) ) ).cache()
        Q = op ( _+_, Q, map( _*a, op ( _-_, map( _*2, transpose(multiply(transpose(E),P)) ),
                                             map( _*b, Q ) ) ) ).cache()
      }

      println(P.count)
      println(Q.count)
    
      println("**** FactorizationSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

      t = System.currentTimeMillis()

      v(sc,"""
         var P: matrix[Double] = matrix();
         var Q: matrix[Double] = matrix();
         var pq: matrix[Double] = matrix();
         var E: matrix[Double] = matrix();

         for i = 0, n-1 do
             for k = 0, d-1 do
                 P[i,k] := random();

         for k = 0, d-1 do
             for j = 0, mm-1 do
                 Q[k,j] := random();

         var steps: Int = 0;
         while ( steps < num_steps ) {
           steps += 1;
           for i = 0, n-1 do
               for j = 0, mm-1 do {
                   pq[i,j] := 0.0;
                   for k = 0, d-1 do
                       pq[i,j] += P[i,k]*Q[k,j];
                   E[i,j] := R[i,j]-pq[i,j];
                   for k = 0, d-1 do {
                       P[i,k] := P[i,k] ^ (2*a*E[i,j]*Q[k,j]);
                       Q[k,j] := Q[k,j] ^ (2*a*E[i,j]*P[i,k]);
                   };
               };
         };

         println(P.count);
         println(Q.count);
        """)

      println("**** FactorizationDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}
