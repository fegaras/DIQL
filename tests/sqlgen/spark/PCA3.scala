import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._

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

    var P = sc.textFile(args(0))
              .map( line => { val a = line.split(",")
                              ((a(0).toLong,a(1).toLong),a(2).toDouble) } ).toDS()
    var n = P.count()
    var d: Int = 4;
    var r: Double = n/d;
    var m: Int = r.toInt+1;
    var z = r-1
    P.createOrReplaceTempView("P")
   
    s(sc,"""
    
    var mean: vector[Double] = vector();
    var sum: matrix[Double] = matrix();

    for i = 0, n-1 do {
       for j = 0, m-1 do
          mean[j] += P[i,j];
    };

     for i = 0, n-1 do {
       mean[i] := 0.0+mean[i]/r;
	};

   for i = 0, d-1 do {
       for j = 0, d-1 do {
          sum[i,j] := 0.0;
          for k = 0, m-1 do
             sum[i,j] += (P[k,i]-mean[i])*(P[k,j]-mean[j]);
       };
    }; 

    for i = 0, d-1 do {
       for j = 0, d-1 do {
	sum[i,j] := 0.0 + sum[i,j]/z;
	};
    };
    println(sum);
    """)

  }
}

