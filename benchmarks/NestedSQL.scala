import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j._

object Test {

  case class X ( A: Int, D: Int )

  case class Y ( B: Int, C: Int )

  def main ( args: Array[String] ) {
    val xfile = args(0)
    val yfile = args(1)
    val output_file = args(2)
    val sparkConf = new SparkConf().setAppName("Spark SQL Nested")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    sparkConf.set("spark.logConf","false")
    sparkConf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val t: Long = System.currentTimeMillis()

    val XC = spark.sparkContext.textFile(xfile)
                     .map(_.split(",")).map(n => X(n(0).toInt,n(1).toInt))
                     .toDF()
    val YC = spark.sparkContext.textFile(yfile)
                     .map(_.split(",")).map(n => Y(n(0).toInt,n(1).toInt))
                     .toDF()
    XC.createOrReplaceTempView("X")
    YC.createOrReplaceTempView("Y")

    var out = spark.sql("""
                     SELECT x.A
                     FROM X x
                     WHERE x.D IN (SELECT y.C FROM Y y WHERE x.A=y.B)
                   """)

    out.write.csv(output_file)

    println("*** Spark SQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }
}
