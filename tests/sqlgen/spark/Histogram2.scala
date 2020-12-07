import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._

case class Color ( red: Int, green: Int, blue: Int )

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

    var P = sc.textFile(args(0)).zipWithIndex.map{
      case (line,i) => {val a = line.split(",")
        (i.toLong, Color(a(0).toInt,a(1).toInt,a(2).toInt)) }}.toDS()    
    val n = P.count()

    P.createOrReplaceTempView("P")


    s(sc,"""

      var R: map[Int,Int] = map();
      var G: map[Int,Int] = map();
      var B: map[Int,Int] = map();

      for i = 0, n-1 do {
          R[P[i].red] += 1;
          G[P[i].green] += 1;
          B[P[i].blue] += 1;
      };

      println(R);
      println(G);
      println(B);

    """)

  }
}

