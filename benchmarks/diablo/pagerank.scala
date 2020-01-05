import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util._
import scala.annotation.tailrec


object PageRank {
  val conf = new SparkConf().setAppName("PageRank")
  val sc = new SparkContext(conf)
  val fs = FileSystem.get(sc.hadoopConfiguration)

  conf.set("dfs.replication","1")
  conf.set("spark.logConf","false")
  conf.set("spark.eventLog.enabled","false")
  LogManager.getRootLogger().setLevel(Level.WARN)

  def build ( vertices: Int, edges: Int, file: String ) {
    val rand = new Random()
    val RMATa = 0.30
    val RMATb = 0.25
    val RMATd = 0.25
    val RMATc = 0.20

    def pickQuadrant ( a: Double, b: Double, c: Double, d: Double ): Int
      = rand.nextDouble() match {
          case x if x < a => 0
          case x if (x >= a && x < a + b) => 1
          case x if (x >= a + b && x < a + b + c) => 2
          case _ => 3
        }

    def chooseCell ( x: Int, y: Int, t: Int ): String
      = if (t <= 1)
           x+","+y
        else {
           val newT = math.round(t.toFloat/2.0).toInt
           pickQuadrant(RMATa, RMATb, RMATc, RMATd) match {
             case 0 => chooseCell(x, y, newT)
             case 1 => chooseCell(x + newT, y, newT)
             case 2 => chooseCell(x, y + newT, newT)
             case 3 => chooseCell(x + newT, y + newT, newT)
           }
        }

     def addEdge ( vn: Int ): String = {
       val v = math.round(vn.toFloat/2.0).toInt
       chooseCell(v,v,v)
     }

    val vn = math.round(Math.pow(2.0,Math.ceil(Math.log(vertices)/Math.log(2.0)))).toInt
    sc.parallelize(1 to edges,1).map( _ => addEdge(vn))
      .saveAsTextFile(file)
  }

  def test ( vertices: Int, num_steps: Int, file: String ) {
   val E = sc.textFile(file)
              .map( line => { val a = line.split(",").toList
                              ((a(0).toLong,a(1).toLong),true) } )

    var t: Long = System.currentTimeMillis()

    val links = E.map(_._1).groupByKey().cache()
    var ranks = links.mapValues(v => 1.0/vertices)

    for (i <- 1 to num_steps) {
        val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
             val size = urls.size
             urls.map(url => (url, rank / size))
        }
        ranks = contribs.reduceByKey(_ + _).mapValues(0.15/vertices + 0.85 * _)
    }
    println(ranks.count)

    println("**** PagerankSpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    v(sc,"""

      var P: vector[Double] = vector();
      var C: vector[Int] = vector();
      var N: Int = vertices;
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

      while (k < num_steps) {
        var Q: matrix[Double] = matrix();
        k += 1;
        for i = 1, N do
          for j = 1, N do
              if (E[i,j])
                 Q[i,j] := P[i];
        for i = 1, N do
            P[i] := (1-b)/N;
        for i = 1, N do
            for j = 1, N do
                P[i] += b*Q[j,i]/C[j];
      };

      println(P.count());

     """)

    println("**** Pagerank run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

   }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val vertices = args(1).toInt
    val edges = args(2).toInt
    val file = "data"

    fs.delete(new Path(file),true)
    build(vertices,edges,file)
    println("*** %d %s  %.2f GB".format(vertices,edges,fs.getContentSummary(new Path(file)).getLength()/(1024.0*1024.0)))

    for ( i <- 1 to repeats )
        test(vertices,1,file)

    fs.delete(new Path(file),true)
    sc.stop()
  }
}
