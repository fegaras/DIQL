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
package edu.uta.diql.core

object inMemory {

  def flatMap[A,B] ( f: A => Traversable[B], S: Traversable[A] ): Traversable[B]
    = S.flatMap(f)

  def groupBy[K,A] ( S: Traversable[(K,A)] ): Traversable[(K,Traversable[A])]
    = S.groupBy{ case (k,a) => k }.mapValues( _.map{ case (k,a) => a })

  def orderBy[K,A] ( S: Traversable[(K,A)] ) ( implicit cmp: Ordering[K] ): Traversable[A]
    = S.toSeq.sortWith{ case ((k1,_),(k2,_)) => cmp.lt(k1,k2) }.map(_._2)

  def reduce[A] ( acc: (A,A) => A, S: Traversable[A] ): A
    = S.reduce(acc)

  def coGroup[K,A,B] ( X: Traversable[(K,A)], Y: Traversable[(K,B)] ): Traversable[(K,(Traversable[A],Traversable[B]))]
    = { val xi = X.map{ case (k,x) => (k,Left(x)) }
        val yi = Y.map{ case (k,y) => (k,Right(y)) }
        val g = groupBy(xi++yi)
        g.map{ case (k,xy)
                => (k,(xy.foldLeft(Nil:List[A]){ case (r,Left(x)) => x::r; case (r,_) => r },
                       xy.foldLeft(Nil:List[B]){ case (r,Right(y)) => y::r; case (r,_) => r })) }
     }

  def cross[A,B] ( X: Traversable[A], Y: Traversable[B] ): Traversable[(A,B)]
    = X.flatMap(x => Y.map(y => (x,y)))

  def merge[A] ( X: Traversable[A], Y: Traversable[A] ): Traversable[A]
    = X++Y
}
