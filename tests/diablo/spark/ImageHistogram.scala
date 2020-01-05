import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

case class Color ( red: Int, green: Int, blue: Int )

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    var P = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              Color(a(0).toInt,a(1).toInt,a(2).toInt) } )

    v(sc,"""

      var R: map[Int,Int] = map();
      var G: map[Int,Int] = map();
      var B: map[Int,Int] = map();

      for p in P do {
          R[p.red] += 1;
          G[p.green] += 1;
          B[p.blue] += 1;
      };

      println("R values");
      R.foreach(println);
      println("G values");
      G.foreach(println);
      println("B values");
      B.foreach(println);

    """)

  }
}

