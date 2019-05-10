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

     var min: int = 1000;
     var closest: int = 0;

     while (exit) {
        for i = 0, N-1 do {
            min = 1000;
            closest = 0;
            for j = 0, K-1 do {
                d = distance(P[i],C[j]);
                if (min > d) {
                   min = d;
                   closest = j;
                };
            };
            sum[closest].X = sum[closest].X + P[i].X;
            sum[closest].Y = sum[closest].Y + P[i].Y;
            count[closest] = count[closest] + 1;
        };
        for i = 0, K-1 do {
            C[i].X = C[i].X + sum[i].X/count[i];
            C[i].Y = C[i].Y + sum[i].Y/count[i];
        };
     };

     """)

   R.foreach(println)

  }
}
