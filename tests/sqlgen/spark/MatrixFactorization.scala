import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    explain(true)

    val n = 3
    val m = 3
    val l = 3
    var a: Double = 0.002;
    var b: Double = 0.02;
	
    var R = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } ).toDS()
    var P = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } ).toDS()
    var Q = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } ).toDS()
   
    R.createOrReplaceTempView("R")
    P.createOrReplaceTempView("P")
    Q.createOrReplaceTempView("Q")

    s(sc,"""
      var pq: matrix[Double] = matrix();
      var E: matrix[Double] = matrix();
      var RR: matrix[Double] = matrix();
      var SS: matrix[Double] = matrix();
      var PP: matrix[Double] = matrix();
      var QQ: matrix[Double] = matrix();
      
      for i = 0, n-1 do
        for j = 0, m-1 do {
          for k = 0, l-1 do
            pq[i,j] += P[i,k]*Q[k,j];
          E[i,j] := R[i,j]-pq[i,j];
             };

      for i = 0, n-1 do
        for j = 0, m-1 do {
           for k = 0, l-1 do {
               RR[i,k] += 0.002*(2*E[i,j]*Q[k,j]-0.02*P[i,k]);
                 };
             };

      for i = 0, n-1 do
          for j = 0, m-1 do {
              PP[i,j] := P[i,j] + RR[i,j];
           };

      for i = 0, n-1 do
        for j = 0, m-1 do {
           for k = 0, l-1 do {
               SS[k,j] += 0.002*(2*E[i,j]*PP[i,k]-0.02*Q[k,j]);
                 };
             };

      for i = 0, n-1 do
          for j = 0, m-1 do {
              QQ[i,j] := Q[i,j] + SS[i,j];
           };

      println(PP);
      println(QQ);
     """)

  }
}

