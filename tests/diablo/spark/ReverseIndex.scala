import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import Math._

object Test {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("reverse-index")
    val sc = new SparkContext(conf)
    val regex = "<a\\s+(?:[^>]*?\\s+)?href=([\"'])(.*?)\\1".r
    var ri = sc.wholeTextFiles("reverseIndex/")
            .map { case (k, v) =>
            (k, regex.findAllMatchIn(v).toList)}
            .flatMap { case (k, v) => v.map(s => (k, s.toString)) }
   
    val file = ri.zipWithIndex.map{ case (line,i) =>
      (i.toLong,(line._1)) }

    val link = ri.zipWithIndex.map{ case (line,i) =>
      (i.toLong,(line._2)) }

    val n = ri.count()

    v(sc,"""
      var R: map[String,String] = map();
      var itr: Int = 0; 
      
      for i = 0, n-1 do {     
        R[link[i]] := "";
      };
      for i = 0, n-1 do {
        R[link[i]] += file[i];
      };
      
      R.foreach(println);
      """)
      sc.stop()
  }
}

