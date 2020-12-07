import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._

case class Dataset(I: Long, J: Long, V: Double)

object Test {
  def main ( args: Array[String] ) {
    val conf = new SparkConf()
	.setAppName("Test")
	.setMaster("local[2]")
    
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    explain(true)


    val P = sc.textFile(args(0)).zipWithIndex.map{
      case (line,i) => {val a = line.split(",")
        ((a(0).toLong, a(1).toLong), Dataset(a(0).toLong, a(1).toLong, a(2).toDouble)) }}.toDS()
    var n = P.count()
    var r: Double = n/3;

    P.createOrReplaceTempView("P")
    P.printSchema()
    s(sc,"""

    var mean: vector[Double] = vector();
    var ri: Int = r.toInt;
    var sum: matrix[Double] = matrix();

    for i = 0, 8 do {
       for j = 0, 2 do
          mean[j] += P[i,j].V;
    };

    for i = 0, 8 do {
       mean[i] := 0.0+mean[i]/3;
	};
    
    for i = 0, 2 do {
       for j = 0, 2 do {
          sum[i,j] := 0.0;
          for k = 0, 2 do
             sum[i,j] += (P[k,i].V - mean[i])*(P[k,j].V-mean[j]);
       };
    };
    
    for i = 0, 2 do {
       for j = 0, 2 do {
	sum[i,j] := 0.0 + sum[i,j]/2;
	};
    };
    println(sum);
    """)

  }
}

