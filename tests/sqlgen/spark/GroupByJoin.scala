import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import Math._

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
    
    //v.txt  
    val V = sc.textFile(args(0)).map( line => { val a = line.split(",");(a(0).toLong,a(1).toLong)}).toDS()
    //k.txt
    val K = sc.textFile(args(1)).zipWithIndex.map{ case (line,i) => (i, line.toLong) }.toDS()

    val n = V.count()


    K.createOrReplaceTempView("K")
    V.createOrReplaceTempView("V")
    
    s(sc,"""
     var W: vector[Double] = vector();

      for i = 0, 9 do {
	  W[K[i]] += V[i];
      };

     println(W);
     """)

  }
}

