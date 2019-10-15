import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import Math._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    var P = sc.textFile(args(0))
              .zipWithIndex.map{ case (line,i)
                                   => { val a = line.split(",")
                                        (i.toLong,(a(0).toDouble,a(1).toDouble)) } }

    var C = sc.textFile(args(1))
              .zipWithIndex.map{ case (line,i)
                                   => { val a = line.split(",")
                                        (i.toLong,(a(0).toDouble,a(1).toDouble)) } }.collect()

    def distance ( x: (Double,Double), y: (Double,Double) ): Double
      = pow(x._1-y._1,2)+pow(x._2-y._2,2)

    case class ArgMin ( index: Long, distance: Double ) {
      def ^ ( x: ArgMin ): ArgMin
        = if (distance <= x.distance) this else x
    }

    v(sc,"""
     var sum: vector[(Double,Double)] = vector();
     var count: vector[Long] = vector();
     var closest: vector[ArgMin] = vector();

     var K: Long = C.length;
     var N: Long = P.count();

     var steps: Int = 0;
     while (steps < 10) {
        steps += 1;
        for i = 0, K-1 do {
            sum[i] := (0.0,0.0);
            count[i] := 0L;
        };
        for i = 0, N-1 do {
            closest[i] := ArgMin(0,10000.0);
            for j = 0, K-1 do
                closest[i] := closest[i] ^ ArgMin(j,distance(P[i],C[j]));
            sum[closest[i].index]#1 += P[i]#1;
            sum[closest[i].index]#2 += P[i]#2;
            count[closest[i].index] += 1L;
        };
        for i = 0, K-1 do {
            //C[i]#1 := sum[i]#1/count[i];
            //C[i]#2 := sum[i]#2/count[i];
            C[i] := (sum[i]#1/count[i],sum[i]#2/count[i]);
        };
     };

     C.foreach(println);

     """)

  }
}
