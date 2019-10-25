import edu.uta.diql._
import org.apache.flink.api.scala._

object Test {

  def main ( args: Array[String] ) {
    val env = ExecutionEnvironment.getExecutionEnvironment

    explain(true)

    val n = args(2).toLong
    val m = n

    var M = env.readTextFile(args(0))
               .map( line => { val a = line.split(",")
                               ((a(0).toLong,a(1).toLong),a(2).toDouble) } )
    var N = env.readTextFile(args(1))
               .map( line => { val a = line.split(",")
                               ((a(0).toLong,a(1).toLong),a(2).toDouble) } )

    v(env,"""

      var R: matrix[Double] = matrix();

      for i = 0, n-1 do
          for j = 0, n-1 do {
               R[i,j] := 0.0;
               for k = 0, m-1 do
                   R[i,j] += M[i,k]*N[k,j];
          };

      R.print;

    """)

  }
}
