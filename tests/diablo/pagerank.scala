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


    var P: RDD[(Int,Double)] = null
    var E: RDD[((Int,Int),Boolean)] = null

    v("""

      external E: matrix[bool];
      external P: vector[double];
      var Q: vector[double];
      var C: vector[int];
      var exit: bool = false;
      var N: int = 100;

      for i = 1, N do {
          C[i] = 0;
          P[i] = 1.0/toDouble(N);
          for j = 1, N do
             if (E[i,j])
                C[i] = C[i] + 1;
      };

      while (!exit) {
        exit = true;
        for i = 1, N do {
            Q[i] = P[i];
            for j = 1, N do
                if (E[j,i])
                   P[i] = P[i] + P[j]/toDouble(C[j]);
            exit = exit && (abs(Q[i]-P[i])/P[i] < 0.01);
        };
      };

     """)

   //P.foreach(println)

  }
}
