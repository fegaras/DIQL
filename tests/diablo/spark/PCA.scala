import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)


    var P = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )
    var n = P.count()

    v(sc,"""

    var mean: vector[Double] = vector();
    var d: Int = 3;
    var r: Double = n/d;
    var ri: Int = r.toInt;
    var sum: matrix[Double] = matrix();

    for i = 0, d-1 do {
       for j = 0, ri-1 do
          mean[i] += P[j,i];
       mean[i] := 0.0+mean[i]/r;
    };
    
    for i = 0, d-1 do {
       for j = 0, d-1 do {
          sum[i,j] := 0.0;
          for k = 0, ri-1 do
             sum[i,j] += (P[k,i]-mean[i])*(P[k,j]-mean[j]);
          sum[i,j] := 0.0 + sum[i,j]/(r-1);
       };
    };    

    sum.foreach(println);

    """)

  }
}
