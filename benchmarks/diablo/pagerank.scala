import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import Math._
import org.apache.log4j._

object Pagerank {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Pagerank")
    val sc = new SparkContext(conf)

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    //explain(true)

    val E = sc.textFile(args(0))
              .map( line => { val a = line.split(",").toList
                              ((a(0).toLong,a(1).toLong),true) } )

    val t: Long = System.currentTimeMillis()

    v(sc,"""

      var P: vector[Double] = vector();
      var Q: vector[Double] = vector();
      var C: vector[Int] = vector();
      var N: Int = args(1).toInt;
      var b: Double = 0.85;

      for i = 1, N do {
          C[i] := 0;
          P[i] := 1.0/N;
      };

      for i = 1, N do
          for j = 1, N do
             if (E[i,j])
                C[i] += 1;

      var k: Int = 0;
      while (k < 10) {
        k += 1;
        for i = 1, N do
            Q[i] := P[i];
        for i = 1, N do
            P[i] := 0.0;
        for i = 1, N do
            for j = 1, N do
                if (E[j,i])
                   P[i] += b*Q[j]/C[j];
        for i = 1, N do
            P[i] += (1-b)/N;
      };

      println(P.count());

     """)

    println("**** Pagerank run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    sc.stop()

  }
}