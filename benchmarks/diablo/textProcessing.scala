import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random


object TextProcessing {
  val conf = new SparkConf().setAppName("TextProcessing")
  val sc = new SparkContext(conf)
  val fs = FileSystem.get(sc.hadoopConfiguration)

  conf.set("dfs.replication","1")
  conf.set("spark.logConf","false")
  conf.set("spark.eventLog.enabled","false")
  LogManager.getRootLogger().setLevel(Level.WARN)

  def build ( length: Int, file: String ) {
    val rand = new Random()

    val max: Int = length/10   // 10 duplicates on the average

    def word (): Int = Math.abs(rand.nextInt())*max

    sc.parallelize(1 to length,1)
      .map{ x => "x"+word() }
      .saveAsTextFile(file)
  }

  def test ( file: String ) {
    val V = sc.textFile(file).flatMap(_.split(","))

    val x = V.first()

    var t: Long = System.currentTimeMillis()

    println(V.filter(_ != x).isEmpty)

    println("**** EqualSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    v(sc,"""
      var eq: Boolean = true;

      for v in V do
          eq := eq && v == x;

      println(eq);

     """)

    println("**** Equal run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    val words = V

    val key1 = "key1"
    val key2 = "key2"
    val key3 = "key3"

    t = System.currentTimeMillis()

    val c1 = words.map{ _ == key1 }.reduce(_||_)
    val c2 = words.map{ _ == key2 }.reduce(_||_)
    val c3 = words.map{ _ == key3 }.reduce(_||_)

    println(c1+" "+c2+" "+c3);

    println("**** StringMatchSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    v(sc,"""

      var c1: Boolean = false;
      var c2: Boolean = false;
      var c3: Boolean = false;

      for w in words do {
          c1 := w == key1;
          c2 := w == key2;
          c3 := w == key3;
      };

      println(c1+" "+c2+" "+c3);

      """)

    println("**** StringMatch run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    val counts = words.map((_,1)).reduceByKey(_+_)

    println(counts.count())

    println("**** WordCountSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    v(sc,"""

      var C: map[String,Int] = map();

      for w in words do
          C[w] += 1;

      println(C.count);

     """)

    println("**** WordCount run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

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
