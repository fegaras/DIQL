import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    var M = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toInt,a(1).toInt),a(2).toDouble) } )
    var N = sc.textFile(args(1))
              .map( line => { val a = line.split(",")
                              ((a(0).toInt,a(1).toInt),a(2).toDouble) } )

    var R: RDD[((Int,Int),Double)] = N

    v("""
 external M: matrix[double];
 external N: matrix[double];
 external R: matrix[double];

 for i = 0, 199 do
    for j = 0, 299 do {
       R[i,j] = 0.0;
       for k = 0, M.cols do
          R[i,j] = R[i,j]+M[i,k]*N[k,j];
    };

     """)

   R.repartition(1).sortByKey().foreach(println)

  }
}
