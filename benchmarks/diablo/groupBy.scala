import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random


case class GB ( K: Long, A: Double )


object GroupBy {
  val conf = new SparkConf().setAppName("GroupBy")
  val sc = new SparkContext(conf)
  val fs = FileSystem.get(sc.hadoopConfiguration)

  conf.set("dfs.replication","1")
  conf.set("spark.logConf","false")
  conf.set("spark.eventLog.enabled","false")
  LogManager.getRootLogger().setLevel(Level.WARN)

  def build ( length: Int, file: String ) {
    val rand = new Random()

    val max: Int = length/10   // 10 duplicates on the average

    sc.parallelize(1 to length,1)
      .map{ x => (Math.abs(rand.nextInt())*max)+","+rand.nextDouble() }
      .saveAsTextFile(file)
  }

  def test ( file: String ) {
    val V = sc.textFile(file).map {
               case line
                 => val a = line.split(",")
                    GB(a(0).toLong, a(1).toDouble)
               }

    var t: Long = System.currentTimeMillis()

    val C = V.map{ case GB(k,v) => (k,v) }.reduceByKey(_+_)

    println(C.count())

    println("**** GroupBySpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    v(sc,"""

      var C: vector[Double] = vector();

      for v in V do
	  C[v.K] += v.A;

      println(C.count);
     
     """)

    println("**** GroupBy run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val length = args(1).toInt
    val file = "data"

    fs.delete(new Path(file),true)
    build(length,file)
    println("*** %d  %.2f GB".format(length,fs.getContentSummary(new Path(file)).getLength()/(1024.0*1024.0)))

    for ( i <- 1 to repeats )
        test(file)

    fs.delete(new Path(file),true)
    sc.stop()
  }
}
