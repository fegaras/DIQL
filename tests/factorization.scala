/* Copyright © 2017 University of Texas at Arlington
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
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  type Matrix = RDD[ ( Double, Int, Int ) ]

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)
    
    def readMatrix ( file: String )
      = sc.textFile(file)
          .map( line => { val Array(v,i,j) = line.split(",")
                          (v.toDouble,i.toInt,j.toInt)
                        } )
    explain(true)
    val a = 0.002
    val b = 0.02
    val iterations = 5

    m("""
      def transpose ( X: Matrix ) =
        select (x,j,i)
        from (x,i,j) <- X;

      // matrix multiplication:
      def multiply ( X: Matrix, Y: Matrix ) =
        select ( +/z, i, j )
        from (x,i,k) <- X, (y,k_,j) <- Y, z = x*y
        where k == k_
        group by (i,j);

      // multiplication by a number:
      def mult ( a: Double, X: Matrix ) =
        select ( a*x, i, j )
        from (x,i,j) <- X;

      // cell-wise addition:
      def Cadd ( X: Matrix, Y: Matrix ) =
        select ( x+y, i, j )
        from (x,i,j) <- X, (y,i_,j_) <- Y
        where i == i_ && j == j_;

      // cell-wise subtraction:
      def Csub ( X: Matrix, Y: Matrix ) =
        select ( x-y, i, j )
        from (x,i,j) <- X, (y,i_,j_) <- Y
        where i == i_ && j == j_;

      // Matrix Factorization using Gradient Descent
      def factorize ( R: Matrix, Pinit: Matrix, Qinit: Matrix ) =
        repeat (E,P,Q) = (R,Pinit,Qinit)
        step ( Csub(R,multiply(P,transpose(Q))),
               Cadd(P,mult(a,Csub(mult(2,multiply(E,transpose(Q))),mult(b,P)))),
               Cadd(Q,mult(a,Csub(mult(2,multiply(E,transpose(P))),mult(b,Q)))) )
        limit iterations
      """)

    q("""
      let M = readMatrix("matrix.txt"),
          I = select (0.5D,i,j) from (_,i,j) <- M,
          (E,L,R) = factorize(M,I,I)
      in multiply(L,transpose(R))
    """).foreach(println)
  }
 }
