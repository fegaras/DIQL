import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j._

object Test {

  case class Edge ( src: Int, dest: Int )

  case class Rank ( id: Int, count: Long, rank: Double )

  val alpha = 0.85

  def rank ( sums: Long, counts: Long ): Double = (1-alpha)+alpha*sums/counts

  def main ( args: Array[String] ) {
    val iterations = args(0).toInt
    val input_file = args(1)
    val output_file = args(2)
    val sparkConf = new SparkConf().setAppName("Spark SQL PageRank")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    sparkConf.set("spark.logConf","false")
    sparkConf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    sqlContext.setConf("spark.sql.shuffle.partitions",args(3))

    val t: Long = System.currentTimeMillis()

    val edges = spark.sparkContext.textFile(input_file)
                     .map(_.split(",")).map(n => Edge(n(0).toInt,n(1).toInt))
                     .toDF()
    edges.createOrReplaceTempView("edges")

    var nodes = spark.sql("""
                     SELECT src as id, count(dest) as count, 0.15 as rank
                     FROM edges
                     GROUP BY src
                   """)
    nodes = nodes.rdd.cache().map(x => Rank(x.getInt(0),x.getLong(1),x.getDecimal(2).doubleValue())).toDF()
    nodes.createOrReplaceTempView("nodes")

    for (i <- 1 to iterations) {
      println(s"Iteration $i")
      nodes = spark.sql("""
                   SELECT n.id, n.count, 0.15+0.85*m.rank as rank
                   FROM nodes n
                   JOIN ( SELECT e.dest, sum(n.rank/n.count) as rank
                          FROM nodes n JOIN edges e ON n.id=e.src
                          GROUP BY e.dest ) m
                     ON m.dest=n.id
                 """)
      // Bug: DataFrames can't do iteration; if you don't force the evaluation with an action, DataFrames will not completely evaluate the pipeline
      nodes = nodes.rdd.cache().map(x => Rank(x.getInt(0),x.getLong(1),x.getDouble(2))).toDF()
      nodes.createOrReplaceTempView("nodes")
    }

    nodes.write.csv(output_file)

    println("*** Spark SQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }
}
