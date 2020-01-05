/*
 * Copyright © 2017 University of Texas at Arlington
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import edu.uta.diql._
import scala.io.Source
import scala.collection.parallel.ParIterable

object Test {

  def main ( args: Array[String] ) {
    val R = Source.fromFile("graph.txt").getLines
              .map( line => { val a = line.split(",").toList
                              ("x"+a.head,a.head.toLong)
                            } ).toList.par
    val S = Source.fromFile("graph.txt").getLines
              .map( line => { val a = line.split(",").toList
                              (a.head.toLong,a.head.toLong,a.tail.map(_.toLong))
                            } ).toList.par

     explain(true)

     val L = List(1,2,3)

     def mymonoid ( x: Int, y: Int ): Int = x+y
     def !! ( x: Double, y: Double ): Double = Math.min(x,y)
     monoid("mymonoid",0)
     monoid("!!",null)

     m("def ff (v: Int) = select y from (x,y) <- R where y < v")

     qs("""
       4::5::List(1,2);
       +/List(1,2,3);
       count/S;
       mymonoid/L;
       L.map(_+1);
       L.reduce(_+_);
       L.map(_+1).reduce(Math.min)
       !!/List(2.3,4.3);
       avg/select i+1 from i <- (1 to 100).toList where i%2 == 0;
       select (i,count/j) from (i,j) <- List((1,"a"),(2,"b"),(1,"c")) group by i;
       select (x,y) from x <- (1 to 10).toList, y <- (10 to 20).toList where x+10==y;
       select (i+1,j) from (i,j,_) <- S where i<3;
       select distinct (i+1,j) from (i,j,_) <- S where i<3;
       select (i+1,m) from (i,2,m@List(1,3)) <- S where i<3;
       select (i,+/xs) from (i,j,xs) <- S;
       select (i,+/xs) from (i,j,xs) <- S order by i+j;
       select (i,+/xs) from (i,j,xs) <- S order by (i desc,i+j,j asc);
       select (i,c) from (i,j,xs) <- S, c = +/xs;
       select (i,avg/xs) from (i,j,xs) <- S where i < count/xs;
       select (i+1,k) from (i,j,xs) <- S, k <- xs where i<3;
       select (i,+/j) from (i,j,_) <- S group by i;
       select (i,+/j) from (i,j,_) <- S group by i having avg/j>2.3D;
       select (i,+/j) from (i,j,_) <- S group by i order by i;
       select (k,l,+/j,avg/i) from (i,j,_) <- S group by (k,l): (i+j,j*3);
       select (x,y) from x <- S, y <- R where x._1==y._2;
       select (x,y) from x <- S, y <-- R where x._1==y._2;
       select (i,j,d) from (i,j,_) <- S, d@(k,m) <-- R where i==m;
       select (x,y) from x <- S, y <-- R where x._1==y._2 && x._2>3 && y._1=="x2";
       select x from n <- (1 to 100).toList, x <- S where x._1==n.toLong;
       select x from x <- S, n <- (1 to 100).toList where x._1==n.toLong;
       select x from n <- (1 to 100).toList, x <- S;
       select x from x <- S, n <- (1 to 100).toList;
       avg/(select i from (i,j,_) <- S where j < 2);
       select (x,(select w from (z,w)<-R where w<y)) from (x,y)<-R;
       select (x,+/(select w from (z,w)<-R where w<y)) from (x,y)<-R;
       some (i,3,_) <- S: i<2;
       all (i,3,_) <- S: i<2;
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
       select (k,avg/j) from x <-- S, (i,j,s) <- S where x._2==j group by k: x._1;
       let x = (select x from x <- S where x._1<2) in x++x;
       repeat s = List(1,2,3) step s.map(_+1) until (+/s) > 60;
       repeat s = select i from (i,_,_) <- S step select i+1 from i <- s until (+/s) > 100
       select x from x <- ff(3)
       """).map{ case e: ParIterable[Any]@unchecked => e.foreach(println); case e => println(e) }

    q("""
    select (i/2.0,z._2,max/xs)
      from (i,j,xs) <- S,
           x <- xs,
           z <- (select (i,+/j) from (i,j,s) <- S group by i)
      where (some k <- xs: k> 3) && i==z._1
    """).foreach(println)

    q("""
    select ( i,
             select (n,w) from (n,_,_) <- S, w <- R where w._2==n && w._2==j )
      from (i,j,xs) <- S,
           z <- S
      where (some k <- xs: k> 3) && i==z._1
    """).foreach(println)
  }
}
