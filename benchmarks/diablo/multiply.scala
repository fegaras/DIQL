import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random


object Multiply {
  val conf = new SparkConf().setAppName("Multiply")
  val sc = new SparkContext(conf)
  val fs = FileSystem.get(sc.hadoopConfiguration)

  conf.set("dfs.replication","1")
  conf.set("spark.logConf","false")
  conf.set("spark.eventLog.enabled","false")
  LogManager.getRootLogger().setLevel(Level.WARN)

  def build ( n: Int, m: Int, file: String ) {
    val rand = new Random()

    def randomMatrix ( n: Int, m: Int, file: String ) {
      val max = 10
      val l = Random.shuffle((0 until n-1).toList)
      val r = Random.shuffle((0 until m-1).toList)
      sc.parallelize(l,1)
        .flatMap{ i => r.map{ j => i+","+j+","+(rand.nextDouble()*max) } }
        .saveAsTextFile(file)
    }

    randomMatrix(n,m,file+1)
    randomMatrix(m,n,file+2)
  }

  def test ( nn: Int, mm: Int, file: String ) {
    var M = sc.textFile(file+1)
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )
    var N = sc.textFile(file+2)
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } )

    var t: Long = System.currentTimeMillis()

    val R = core.GroupByJoin.groupByJoin[((Long,Long),Double),((Long,Long),Double),Long,Long,Long,Double](
                { case ((i,j),m) => i },
                { case ((i,j),n) => j },
                { case ((_,m),(_,n)) => m*n },
                _+_,
                M.map{ case x@((i,j),m) => (j,x)},
                N.map{ case x@((i,j),b) => (i,x)}
             )

    println(R.count)

    println("**** MultiplySpark run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    v(sc,"""

      var R: matrix[Double] = matrix();

      for i = 0, nn-1 do
          for j = 0, nn-1 do {
               R[i,j] := 0.0;
               for k = 0, mm-1 do
                   R[i,j] += M[i,k]*N[k,j];
          };

      println(R.count);

    """)

    println("**** Multiply run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val n = args(1).toInt
    val m = args(2).toInt
    val file = "data"

    fs.delete(new Path(file+1),true)
    fs.delete(new Path(file+2),true)
    build(n,m,file)
    val size = fs.getContentSummary(new Path(file+1)).getLength()
                  + fs.getContentSummary(new Path(file+2)).getLength()
    println("*** %d %d  %.2f GB".format(n,m,size/(1024.0*1024.0*1024.0)))

    for ( i <- 1 to repeats )
        test(n,m,file)

    fs.delete(new Path(file+1),true)
    fs.delete(new Path(file+2),true)
    sc.stop()
  }
}
