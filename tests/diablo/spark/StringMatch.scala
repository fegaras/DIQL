import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("StringMatch")
    val sc = new SparkContext(conf)

    val w = sc.textFile(args(0))
              .zipWithIndex.map{ case (line,i) =>  (i, line)}

    val k = sc.textFile(args(1))
              .zipWithIndex.map{ case (line,i) =>  (i, line)}

    val N = w.count()
    val M = k.count()

    explain(true)

    v(sc,"""
      for i = 0, N-1 do      
	 for j = 0, M-1 do
	    if (w[i] == k[j])
               println(k[j]+" true");
         """)
    sc.stop()
  }
}
