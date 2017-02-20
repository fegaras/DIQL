import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)
    val R = sc.textFile("graph.txt")
              .map( line => { val a = line.split(",").toList
                              ("x"+a.head,a.head.toLong)
                            } )
    val S = sc.textFile("graph.txt")
              .map( line => { val a = line.split(",").toList
                              (a.head.toLong,a.head.toLong,a.tail.map(_.toLong))
                            } )

     debug(true)

     qs("""
       select (i+1,j) from (i,j,_) <- S where i<3;
       select (i,+/xs) from (i,j,xs) <- S;
       select (i,c) from (i,j,xs) <- S, c = +/xs;
       select (i,avg/xs) from (i,j,xs) <- S where i < count/xs;
       select (i+1,k) from (i,j,xs) <- S, k <- xs where i<3;
       select (x,y) from x <- S, y <- R where x._1==y._2;
       select (x,y) from x <- S, y <-- R where x._1==y._2;
       select (i,j,d) from (i,j,_) <- S, d@(k,m) <-- R where i==m;
       select (x,y) from x <- S, y <-- R where x._1==y._2 && x._2>3 && y._1=="x2";
       avg/(select i from (i,j,_) <- S where j < 2);
       select (x,+/(select x._1 from y <- S where x._2==y._2)) from x <- S;
       select (i,j) from (i,j,xs) <- S where (+/xs) < 3;
       select x from x <- S where (+/select y._1 from y <- S where x._1==y._1)<2;
       select (x,select (k,count/y) from y <- (select z from z <- S where z._1>3)
           where x._2==y._2 group by k: y._1) from x <- S;
       select (x,select (k,count/y) from y <- S where x._2==y._2 group by k: y._1) from x <- S;
       select x from x <- S where some y <- R: x._2==y._2;
       select x from x <- S where all y <- (select y from y <- R where x._2==y._2): y._1=="x2";
       select (x,y,z) from x <- S, y <- R, z <-- S where x._2==y._2;
       select (x,y,z) from x <- S, y <- R, z <- S, w <- S where x._2==y._2 && y._2==z._1 && w._2==z._2;  
       select (x,y,z) from x <- S, y <- R, z <- S where x._2==y._2 && y._2== z._1;
       select (x,y) from x <- S, y <- R where y._1=="x2" && x._2==y._2 && x._1==45;
       select (k,avg/j) from x <-- S, (i,j,s) <- S where x._2==j group by k: x._1
       """).map{ case e: RDD[Any] => e.foreach(println); case e => println(e) }

    q("""
     select (i/2.0,max/xs)
      from (i,j,xs) <- sc.textFile("graph.txt")
                         .map( line => let a = line.split(",").toList
                                       in ( a.head.toLong, a.head.toLong,
                                            a.tail.map(x => x.toLong )) )
    """).foreach(println)

    q("""
    select (i/2.0,z._2,max/xs)
      from (i,j,xs) in S,
           x in xs,
           z in (select (i,+/j) from (i,j,s) in S group by i)
      where (some k in xs: k> 3) && i==z._1
    """).foreach(println)

  }
}
