import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    //explain(true)


    var P = sc.textFile("ih.txt")
              .map( line => { val a = line.split(",")
                              (a(0).toInt,a(1).toInt,a(2).toInt) } )

    val Ri = P.zipWithIndex.map{ case (line,i) =>
      (i.toLong,(line._1)) }

    val Gi = P.zipWithIndex.map{ case (line,i) =>
      (i.toLong,(line._2)) }

    val Bi = P.zipWithIndex.map{ case (line,i) =>
      (i.toLong,(line._3)) }
    
    val N = P.count()

    v(sc,"""

      var R: map[Int,Int] = map();
      var G: map[Int,Int] = map();
      var B: map[Int,Int] = map();

      for r in Ri do {
         R[r] := 0;
       };
      for r in Ri do {
        R[r] += 1;
      };
      
      for g in Gi do {
         G[g] := 0;
       };
      for g in Gi do {
        G[g] += 1;
      };

     for b in Bi do {
         B[b] := 0;
       };
      for b in Bi do {
        B[b] += 1;
      };

      println("R values");
      R.foreach(println);
      println("G values");
      G.foreach(println);
      println("B values");
      B.foreach(println);

    """)

  }
}

