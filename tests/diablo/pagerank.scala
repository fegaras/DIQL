import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import Math._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    explain(true)

    var E = sc.textFile(args(0))
              .flatMap( line => { val a = line.split(",").toList
                                  a.tail.map(x => ((a.head.toInt,x.toInt),true)) } )

    v(sc,"""

      var P: vector[double] = vector();
      var Q: vector[double] = vector();
      var C: vector[int] = vector();
      var exit: bool = false;
      var N: int = 100;
      var b: double = 0.85;

      for i = 1, N do {
          C[i] := 0;
          P[i] := 1.0/N;
          for j = 1, N do
             if (E[i,j])
                C[i] := C[i] + 1;
      };

      while (!exit) {
        exit := true;
        for i = 1, N do {
            Q[i] := P[i];
            for j = 1, N do
                if (E[j,i])
                   P[i] := P[i] + (1/N)*(1-b) + b*P[j]/C[j];
            exit := exit && (abs(Q[i]-P[i])/P[i] < 0.01);
        };
        println("@@@@ "+P);
      };

     """)

  }
}
