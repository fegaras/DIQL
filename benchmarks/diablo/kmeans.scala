import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import scala.util.Random


object KMeans {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toLong
    val num_steps = 1

    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)
    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val rand = new Random()

    def getd (): Double = {
      val v = rand.nextDouble()*20.0D
      if (v.toInt % 2 == 0) getd() else v
    }

    val points = sc.parallelize(1L to length/100)
                   .flatMap{ i => (1 to 100).map{ i => (getd(),getd()) } }
                   .cache()

    val size = sizeof((1.0D,1.0D))
    println("*** %d  %.2f GB".format(length,length.toDouble*size/(1024.0*1024.0*1024.0)))

    var initial_centroids
          = (for { i <- 0 to 9; j <- 0 to 9 }
             yield ((i*2+1.2).toDouble,(j*2+1.2).toDouble)).toArray

    var centroids = initial_centroids

    def distance ( x: (Double,Double), y: (Double,Double) ): Double
      = Math.sqrt((x._1-y._1)*(x._1-y._1)+(x._2-y._2)*(x._2-y._2))

    def test () {
      case class ArgMin ( index: Long, distance: Double ) {
        def ^ ( x: ArgMin ): ArgMin
          = if (distance <= x.distance) this else x
      }

      case class Avg ( sum: (Double,Double), count: Long ) {
        def ^^ ( x: Avg ): Avg
          = Avg((sum._1+x.sum._1,sum._2+x.sum._2),count+x.count)
        def value(): (Double,Double)
          = (sum._1/count,sum._2/count)
      }

      var t: Long = System.currentTimeMillis()

      try {
      for ( i <- 1 to num_steps ) {
         val cs = sc.broadcast(centroids)
         centroids = points.map { p => (cs.value.minBy(distance(p,_)), Avg(p,1)) }
                           .reduceByKey(_^^_)
                           .map(_._2.value())
                           .collect()
      }
      println(centroids.length)

      println("**** KMeansSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      var P = points.zipWithIndex.map{ case (p,i) => (i.toLong,p) }

      var C = initial_centroids.zipWithIndex.map{ case (p,i) => (i.toLong,p) }

      val K = C.length
      val N = P.count()

      var avg = (1 to K).map{ i => (i.toLong-1,Avg((0.0,0.0),0)) }.toArray

      t = System.currentTimeMillis()

      v(sc,"""
        var closest: vector[ArgMin] = vector();

        var steps: Int = 0;
        while (steps < num_steps) {
           steps += 1;
           for i = 0, N-1 do {
               closest[i] := ArgMin(0,10000.0);
               for j = 0, K-1 do
                   closest[i] := closest[i] ^ ArgMin(j,distance(P[i],C[j]));
               avg[closest[i].index] := avg[closest[i].index] ^^ Avg(P[i],1);
           };
           for i = 0, K-1 do
               C[i] := avg[i].value();
        };
        """)
      println(C.length)

      println("**** KMeansDiablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    }

    for ( i <- 1 to repeats )
      test()

    sc.stop()
  }
}
