import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("StringMatch")
    val sc = new SparkContext(conf)

    val words = sc.textFile(args(0))
                  .flatMap( line => line.split(" ") )

    val keys = sc.textFile(args(1)).collect()

    explain(true)

    v(sc,"""

      var C: map[String,Boolean] = map();

      for k in keys do {
          C[k] := false;
          for w in words do
              if (w == k)
                 C[k] := true;
      };

         """)

  }
}
