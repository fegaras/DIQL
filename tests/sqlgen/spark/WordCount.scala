import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import Math._

case class wclass(W: String)

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    explain(true)

    val D = sc.textFile(args(0))
      .flatMap( line => line.split(" ") ).zipWithIndex.map{ case (line,i) => (i,wclass(line)) }.toDS()

    D.createOrReplaceTempView("D")

    s(sc,"""

      var C: map[String,Int] = map();

      for i = 0, 5 do
           C[D[i].W] += 1;
      
      println(C);
     """)
    
  }
}
