import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd._

case class R ( K: Long, A:Double)

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    val V = sc.textFile(args(0)).zipWithIndex.map {
               case (line,i)
                 => val a = line.split(",")
                    (i.toLong, R(a(0).toLong, a(1).toDouble))
               }

    val n = V.count()

    v(sc,"""

      var C: vector[Double] = vector();

      for i = 0, n-1 do {
	  C[V[i].K] += V[i].A;

      C.foreach(println);

      };
     
     """)

  }
}
