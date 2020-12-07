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

    val n = args(2).toLong
    val m = n
    
    val M = sc.textFile(args(0)).zipWithIndex.map{
      case (line,i) => {val a = line.split(",")
        ((a(0).toLong, a(1).toLong), a(2).toDouble) }}.toDS()

    val N = sc.textFile(args(1)).zipWithIndex.map{
      case (line,i) => {val a = line.split(",")
        ((a(0).toLong, a(1).toLong), a(2).toDouble) }}.toDS()

    M.createOrReplaceTempView("M")
    N.createOrReplaceTempView("N")


    s(sc,"""
     var R: matrix[Double] = matrix();
      for i = 0, 10 do
        for j = 0, 10 do
            R[i,j] := M[i,j] + N[i,j]; 
      println(R);
    """)

  }
}

