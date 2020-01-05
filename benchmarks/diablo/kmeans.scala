import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random


object KMeans {
  val conf = new SparkConf().setAppName("KMeans")
  val sc = new SparkContext(conf)
  val fs = FileSystem.get(sc.hadoopConfiguration)

  conf.set("dfs.replication","1")
  conf.set("spark.logConf","false")
  conf.set("spark.eventLog.enabled","false")
  LogManager.getRootLogger().setLevel(Level.WARN)

  var initial_centroids = Array[(Double,Double)]()

  def build ( length: Int, file: String ) {
    val rand = new Random()

    def getd (): Double = {
      val v = rand.nextDouble()*20.0D
      if (v.toInt % 2 == 0) getd() else v
    }

    initial_centroids
      = (for { i <- 0 to 9; j <- 0 to 9 }
         yield ((i*2+1.2).toDouble,(j*2+1.2).toDouble)).toArray

    sc.parallelize(1 to length,1)
      .map{ x => getd()+","+getd() }
      .saveAsTextFile(file)
  }

  def test ( num_steps: Int, file: String ) {

    var centroids = initial_centroids

    def distance ( x: (Double,Double), y: (Double,Double) ): Double
      = Math.sqrt((x._1-y._1)*(x._1-y._1)+(x._2-y._2)*(x._2-y._2))

    def closest ( p: (Double,Double) ): (Double,Double)
      = centroids.minBy(distance(p,_))

    def average ( s: Iterable[Double] ): Double
      = s.sum/s.size

    val points = sc.textFile(file).map {
                        line => val a = line.split(",")
                                (a(0).toDouble, a(1).toDouble)
                    }

    var t: Long = System.currentTimeMillis()

    for ( i <- 1 to num_steps ) {
      val cs = sc.broadcast(centroids)
      centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }
                        .groupByKey().map {
                              case (c, vs) => (average(vs.map(_._1)),
                                               average(vs.map(_._2)))
                          }.collect()
    }

    println(centroids.length)

    println("**** KMeansSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

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

    println("**** KMeans run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toInt
    val file = "data"

    fs.delete(new Path(file),true)
    build(length,file)
    println("*** %d  %.2f GB".format(length,fs.getContentSummary(new Path(file)).getLength()/(1024.0*1024.0)))

    for ( i <- 1 to repeats )
        test(1,file)

    fs.delete(new Path(file),true)
    sc.stop()
  }
}
