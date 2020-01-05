import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {
  def main ( args: Array[String] ) {

    val conf = new SparkConf().setAppName("reverse-index")
    val sc = new SparkContext(conf)

    val regex = "<a\\s+(?:[^>]*?\\s+)?href=([\"'])(.*?)\\1".r

    var links = sc.wholeTextFiles("reverseIndex/")
                  .map{ case (k, v) => (k, regex.findAllMatchIn(v).toList) }
                  .flatMap{ case (k, v) => v.map(s => (k, s.toString)) }

    explain(true)

    v(sc,"""

      var R: map[String,String] = map();

      for link in links do
          R[link._1] += link._2 + " ";

      R.foreach(println);

      """)
  }
}
