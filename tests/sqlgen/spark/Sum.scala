import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf()
      .setAppName("Spark SQL Sum")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    explain(true)

    val A = sc.textFile(args(0)).zipWithIndex.map{ case (line,i) => (i, line.toDouble) }.toDS()

    val n = A.count()
    A.createOrReplaceTempView("A")


    s(sc,"""
      var sum: Double = 0.0;

      for i = 0, 10 do {
	      sum += A[i];
      };

     println(sum);
     """)

  }
}
