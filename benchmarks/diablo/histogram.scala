import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random


case class Color ( red: Int, green: Int, blue: Int )


object Histogram {
  val conf = new SparkConf().setAppName("Histogram")
  val sc = new SparkContext(conf)
  val fs = FileSystem.get(sc.hadoopConfiguration)

  conf.set("dfs.replication","1")
  conf.set("spark.logConf","false")
  conf.set("spark.eventLog.enabled","false")
  LogManager.getRootLogger().setLevel(Level.WARN)

  def build ( length: Int, file: String ) {
    val rand = new Random()

    def byte () = Math.abs(rand.nextInt()) % 256

    sc.parallelize(1 to length,1)
      .map{ x => byte()+","+byte()+","+byte() }
      .saveAsTextFile(file)
  }

  def test ( file: String ) {
    val P = sc.textFile(file)
              .map( line => { val a = line.split(",")
                              Color(a(0).toInt,a(1).toInt,a(2).toInt) } )

    var t: Long = System.currentTimeMillis()

    val R = P.map(_.red).countByValue()
    val G = P.map(_.green).countByValue()
    val B = P.map(_.blue).countByValue()

    println(R.size)
    println(G.size)
    println(B.size)

    println("**** HistogramSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    v(sc,"""

      var R: map[Int,Int] = map();
      var G: map[Int,Int] = map();
      var B: map[Int,Int] = map();

      for p in P do {
          R[p.red] += 1;
          G[p.green] += 1;
          B[p.blue] += 1;
      };

      println(R.count);
      println(G.count);
      println(B.count);

    """)

    println("**** Histogram run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toInt
    val file = "data"

    fs.delete(new Path(file),true)
    build(length,file)
    println("*** %d  %.2f GB".format(length,fs.getContentSummary(new Path(file)).getLength()/(1024.0*1024.0*1024.0)))

    for ( i <- 1 to repeats )
        test(file)

    fs.delete(new Path(file),true)
    sc.stop()
  }
}
