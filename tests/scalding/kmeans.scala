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
import com.twitter.scalding._


object Test extends ExecutionApp {
    explain(true)

    case class Point ( X: Double, Y: Double ) extends Comparable[Point] {
      def compareTo ( that: Point ): Int = (if (X != that.X) X-that.X else Y-that.Y).toInt
    }

    def distance ( x: Point, y: Point ): Double
      = Math.sqrt(Math.pow(x.X-y.X,2)+Math.pow(x.Y-y.Y,2))

    val pointsDS = TypedPipe.from(TextLine("graph.txt"))

    def job: Execution[Unit] = {
    q("""let points = pointsDS.map( _.split(",") )
                              .map( p => Point(p(0).toDouble,p(1).toDouble) )
         in repeat centroids = Array( Point(0,0), Point(10,0), Point(0,10), Point(10,10) )
            step select Point( avg/x, avg/y )
                 from p@Point(x,y) <- points
                 group by k: ( select c
                               from c <- centroids
                               order by distance(c,p) ).head
            limit 10
      """).debug.writeExecution(TypedTsv("results/output"))
  }
}
