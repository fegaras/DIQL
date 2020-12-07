import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf()
      .setAppName("Test")
      
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    explain(true)
   
    val W = sc.textFile(args(0)).zipWithIndex.map{ case (line,i) =>  (i, line)}.toDS()
    val keys = List("key1", "key2", "key3")
    val K = keys.zipWithIndex.map{ case (line,i) =>  (i.toLong, line)}.toDS()
    
    val w = W.count()
    val k = K.count()

    W.createOrReplaceTempView("W")
    K.createOrReplaceTempView("K")
    
    s(sc,"""
     var C: vector[String] = vector();

      for i = 0, w-1 do {
	  for j = 0, k-1 do
           if (W[i] == K[j])
            C[j] := K[j];
     };
     println(C); 
     """)
  }
}
