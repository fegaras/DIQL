import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._

case class Cclass ( K: Long, A:Double)

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


    val V = sc.textFile(args(0)).zipWithIndex.map{
      case (line,i) => {val a = line.split(",")
        (i.toLong, Cclass(a(0).toLong, a(1).toDouble)) }}.toDS()

    V.createOrReplaceTempView("V")


    s(sc,"""

      var C: vector[Double] = vector();

      for i = 0, 10 do {
      	 C[V[i].K] += V[i].A;
      };

      println(C);
     """)

  }
}
