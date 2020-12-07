import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random

object MatrixFactorization {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val n = args(1).toInt
    val m = args(2).toInt
    val d = 2
    val mm = m
    val x = m
    val a = 0.002
    val b = 0.02

    val conf = new SparkConf().setAppName("LinearRegression")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

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
    val Rds = R.toDS()
    Rds.createOrReplaceTempView("Rds")
  
    var P = sc.parallelize((0 to n-1).flatMap( i => (0 to d-1).map {
                    case j => ((i.toLong,j.toLong),random()) } )) 
              .cache()
    val Pds = P.toDS()
    Pds.createOrReplaceTempView("Pds")
   
    var Q = sc.parallelize((0 to d-1).flatMap( i => (0 to m-1).map {
                    case j => ((i.toLong,j.toLong),random()) } )) 
              .cache()
    val Qds = Q.toDS()
    Qds.createOrReplaceTempView("Qds")

    val size = sizeof(((1L,1L),1))
    println("*** %d %d  %.2f GB".format(n,m,(n.toDouble*m)*size/(1024.0*1024.0*1024.0)))

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

    def multiply(x: RDD[((Long,Long),Double)], y: RDD[((Long,Long),Double)] ): RDD[((Long,Long),Double)]
      = x.map{ case ((i,j),m) => (j,(i,m)) }
         .join( y.map{ case ((i,j),n) => (i,(j,n)) } )
         .map{ case (k,((i,m),(j,n))) => ((i,j),m*n) }
         .reduceByKey(_+_)
 
    
    def test () {
      var t: Long = System.currentTimeMillis()

      try {
        val E = R.cogroup( multiply(P,Q) )
                 .flatMap{ case (k,(ms,ns)) => if (!ms.isEmpty && !ns.isEmpty)
                                                  List((k,ns.head-ms.head))
                                               else Nil }.cache()
        P = op ( _+_, P, map( _*a, op ( _-_, map( _*2, multiply(E,transpose(Q)) ),
                                             map( _*b, P ) ) ) ).cache()
        Q = op ( _+_, Q, map( _*a, op ( _-_, map( _*2, transpose(multiply(transpose(E),P)) ),
                                             map( _*b, Q ) ) ) ).cache()
      
      println(P.count)
      println(Q.count)
       println("**** SparkRDD run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""
       var pq: matrix[Double] = matrix();
      var E: matrix[Double] = matrix();
      var RR: matrix[Double] = matrix();
      var SS: matrix[Double] = matrix();
      var PP: matrix[Double] = matrix();
      var QQ: matrix[Double] = matrix();
      
      for i = 0, n-1 do
        for j = 0, x-1 do {
          for k = 0, d-1 do
            pq[i,j] += P[i,k]*Q[k,j];
          E[i,j] := R[i,j]-pq[i,j];
             };

      for i = 0, n-1 do
        for j = 0, x-1 do {
           for k = 0, d-1 do {
               RR[i,k] += 0.002*(2*E[i,j]*Q[k,j]-0.02*P[i,k]);
                 };
             };

      for i = 0, n-1 do
          for j = 0, x-1 do {
              PP[i,j] := P[i,j] + RR[i,j];
           };

      for i = 0, n-1 do
        for j = 0, x-1 do {
           for k = 0, d-1 do {
               SS[k,j] += 0.002*(2*E[i,j]*PP[i,k]-0.02*Q[k,j]);
                 };
             };

      for i = 0, n-1 do
          for j = 0, x-1 do {
              QQ[i,j] := Q[i,j] + SS[i,j];
           };

      println(PP.count);
      println(QQ.count);       
      """)

      println("**** Diablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
   
      t = System.currentTimeMillis()

      try {
      s(sc,"""
       var pq: matrix[Double] = matrix();
      var E: matrix[Double] = matrix();
      var RR: matrix[Double] = matrix();
      var SS: matrix[Double] = matrix();
      var PP: matrix[Double] = matrix();
      var QQ: matrix[Double] = matrix();
      
      for i = 0, n-1 do
        for j = 0, x-1 do {
          for k = 0, d-1 do
            pq[i,j] += Pds[i,k]*Qds[k,j];
          E[i,j] := Rds[i,j]-pq[i,j];
             };

      for i = 0, n-1 do
        for j = 0, x-1 do {
           for k = 0, d-1 do {
               RR[i,k] += 0.002*(2*E[i,j]*Qds[k,j]-0.02*Pds[i,k]);
                 };
             };

      for i = 0, n-1 do
          for j = 0, x-1 do {
              PP[i,j] := Pds[i,j] + RR[i,j];
           };

      for i = 0, n-1 do
        for j = 0, x-1 do {
           for k = 0, d-1 do {
               SS[k,j] += 0.002*(2*E[i,j]*PP[i,k]-0.02*Qds[k,j]);
                 };
             };

      for i = 0, n-1 do
          for j = 0, x-1 do {
              QQ[i,j] := Qds[i,j] + SS[i,j];
           };

      println(PP.count);
      println(QQ.count);
       """)

      println("**** SQLGen run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()
      try{
      var pq = spark.sql("SELECT Pds._1._1 AS _1_1,  Qds._1._2 AS _1_2, SUM(Pds._2 * Qds._2) AS _2 FROM Qds JOIN Pds ON Qds._1._1 == Pds._1._2 GROUP BY Pds._1._1, Qds._1._2");
    pq.createOrReplaceTempView("pq");
    var E = spark.sql("SELECT Rds._1._1 AS _1_1, Rds._1._2 AS _1_2 , Rds._2 - pq._2 AS _2  FROM pq JOIN Rds ON pq._1_2 == Rds._1._2  AND pq._1_1 == Rds._1._1");
    E.createOrReplaceTempView("E");
    var RR = spark.sql("SELECT E._1_1 AS _1_1,  Qds._1._1 AS _1_2, SUM(0.002 * ((2 * E._2) * (Qds._2)) - (0.02 * Pds._2)) AS _2 FROM Pds JOIN Qds ON Pds._1._2 == Qds._1._1  JOIN E ON Pds._1._1 == E._1_1  AND Qds._1._2 == E._1_2 GROUP BY E._1_1, Qds._1._1");
    RR.createOrReplaceTempView("RR");
    var PP = spark.sql("SELECT Pds._1._1 AS _1_1, Pds._1._2 AS _1_2 , Pds._2 + RR._2 AS _2  FROM RR JOIN Pds ON RR._1_2 == Pds._1._2  AND RR._1_1 == Pds._1._1");
    PP.createOrReplaceTempView("PP");
    println(PP.count)
    var SS = spark.sql("SELECT PP._1_2 AS _1_1,  E._1_2 AS _1_2, SUM(0.002 * ((2 * E._2) * (PP._2)) - (0.02 * Qds._2)) AS _2 FROM Qds JOIN E ON Qds._1._2 == E._1_2  JOIN PP ON Qds._1._1 == PP._1_2  AND PP._1_1 == E._1_1 GROUP BY PP._1_2, E._1_2");
    SS.createOrReplaceTempView("SS");
    var QQ = spark.sql("SELECT Qds._1._1, Qds._1._2 AS _1 , Qds._2 + SS._2 AS _2  FROM SS JOIN Qds ON SS._1_2 == Qds._1._2  AND SS._1_1 == Qds._1._1");
    QQ.createOrReplaceTempView("QQ")
    println(PP.count);
    println("**** SparkSQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      }catch { case x: Throwable => println(x) }
 }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}

