/**
  * Created by ashiqimran on 3/7/17.
  */

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkSQLExample {
  def main(args: Array[String]): Unit = {
    // SparkConf sparkConf = new SparkConf().setMaster("local[2]");
    val spark = SparkSession
      .builder()
      .appName("Java Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate();

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = spark.read.json("people.json")
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    //df.show()
    sqlDF.show()
  }
}
