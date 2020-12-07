import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd._

case class words (W: String)
case class keys (K:String)

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
   
    val W = sc.textFile(args(0)).zipWithIndex.map{ case (line,i) =>  (i, words(line))}.toDS()
    val K = sc.textFile(args(1)).zipWithIndex.map{ case (line,i) =>  (i, keys(line))}.toDS()
    
    val w = W.count()
    val k = K.count()

    W.createOrReplaceTempView("W")
    K.createOrReplaceTempView("K")
    
    s(sc,"""
      var C: map[String,Int] = map();

      for j = 0, k-1 do{
          C[K[j].K] := 0;
            for i = 0, w-1 do
              if (W[i].W == K[j].K)
                 C[K[j].K] += 1;
      };
      println(C);
     """)
  }
}
