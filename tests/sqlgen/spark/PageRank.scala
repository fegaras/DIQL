import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import Math._

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
    var n: Int = args(1).toInt;
    var b: Double = 0.8;
    var c: Double = b/n;
    val E = sc.textFile(args(0))
              .map( line => { val a = line.split(",").toList
                              ((a(0).toLong,a(1).toLong),true)}).toDS()
    E.createOrReplaceTempView("E")
    val R = sc.parallelize(1L to n).map(v => (v,(1.0-b)/n)).toDS()
    R.createOrReplaceTempView("R")
    
    s(sc,"""
      var C: vector[Int] = vector();
      var S: vector[Double] = vector();
      var I: vector[Double] = vector();

      for i = 1, n do
         for j = 1, n do
            if (E[i,j])
                C[i] += 1;
      
      for i = 1, n do{
         for j = 1, n do{
            if (E[j,i])
               S[i] += c/C[j];
                };
           };

      for i = 1, n do
         I[i] += S[i] + R[i];
      
      println(I);
     """)

  }
}
