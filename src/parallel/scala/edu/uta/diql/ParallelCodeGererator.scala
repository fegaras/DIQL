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

import scala.collection.parallel.immutable.ParIterable
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox.Context


abstract class ParallelCodeGenerator extends DistributedCodeGenerator {
  import c.universe.{Expr=>_,_}
  import AST._

  override def typeof ( c: Context ) = c.typeOf[ParIterable[_]]

  override def mkType ( c: Context ) ( tp: c.Tree ): c.Tree = {
    import c.universe._
    tq"ParIterable[$tp]"
  }

  /** Is tp a data stream? */
  override def isStream ( c: Context ) ( tp: c.Type ): Boolean = false

  /** Implementation of the algebraic operations in Scala's Parallel library
   */
  def flatMap[A,B] ( f: (A) => TraversableOnce[B], S: ParIterable[A] ): ParIterable[B]
    = S.flatMap(f)

  def flatMap2[A,B] ( f: (A) => ParIterable[B], S: ParIterable[A] ): ParIterable[B]
    = S.flatMap(f(_).toSeq)

  // bogus; used for type-checking only
  def flatMap[A,B] ( f: (A) => ParIterable[B], S: Traversable[A] ): ParIterable[B]
    = f(S.head)

  def groupBy[K,A] ( S: ParIterable[(K,A)] ): ParIterable[(K,Iterable[A])]
    = S.groupBy(_._1).map{ case (k,s) => (k,s.map(_._2).toList) }.toIterable

  def orderBy[K,A] ( S: ParIterable[(K,A)] ) ( implicit ord: Ordering[K] ): ParIterable[A]
    = S.toList.sortBy(_._1).map(_._2).par  // no parallel sorting in Scala

  def reduce[A] ( acc: (A,A) => A, S: ParIterable[A] ): A
    = S.reduce(acc)

  def coGroup[K,A,B] ( X: ParIterable[(K,A)], Y: ParIterable[(K,B)] ): ParIterable[(K,(Iterable[A],Iterable[B]))]
    = { val gx =  X.groupBy(_._1).mapValues(_.map(_._2).toList)
        Y.groupBy(_._1).map{ case (k,ys) => (k,(if (gx.contains(k)) gx(k) else Nil,ys.map(_._2).toList)) }.toIterable
      }

  def coGroup[K,A,B] ( X: Traversable[(K,A)], Y: ParIterable[(K,B)] ): ParIterable[(K,(Iterable[A],Iterable[B]))]
    = { val gx = X.groupBy(_._1).mapValues(_.map(_._2).toIterable)
        Y.groupBy(_._1).map{ case (k,ys) => (k,(if (gx.contains(k)) gx(k) else Nil,ys.map(_._2).toList)) }.toIterable
      }

  def coGroup[K,A,B] ( X: ParIterable[(K,A)], Y: Traversable[(K,B)] ): ParIterable[(K,(Iterable[A],Iterable[B]))]
    = { val gy = Y.groupBy(_._1).mapValues(_.map(_._2).toIterable)
        X.groupBy(_._1).map{ case (k,xs) => (k,(xs.map(_._2).toList,if (gy.contains(k)) gy(k) else Nil)) }.toIterable
      }

  def cross[A,B] ( X: ParIterable[A], Y: ParIterable[B] ): ParIterable[(A,B)]
    = { val ys = Y.toSeq
        X.flatMap( x => ys.map(y => (x,y)) )
      }

  def cross[A,B] ( X: Traversable[A], Y: ParIterable[B] ): ParIterable[(A,B)]
    = Y.flatMap( y => X.map(x => (x,y)) )

  def cross[A,B] ( X: ParIterable[A], Y: Traversable[B] ): ParIterable[(A,B)]
    = X.flatMap( x => Y.map(y => (x,y)) )

  def merge[A] ( X: ParIterable[A], Y: ParIterable[A] ): ParIterable[A]
    = X++Y

  def collect[A: ClassTag] ( X: ParIterable[A] ): Array[A]
    = X.toArray

  def cache[A] ( X: ParIterable[A] ): ParIterable[A]
    = X

  def head[A] ( X: ParIterable[A] ): A = X.head

  override def codeGen ( e: Expr, env: Environment ): c.Tree
    = if (!isDistributed(e))
        super.codeGen(e,env,codeGen)
      else e match {
        case flatMap(Lambda(p,Elem(b)),x)
          if irrefutable(p)
          => val pc = code(p)
             val (_,tp,xc) = typedCode(x,env,codeGen)
             val bc = codeGen(b,add(p,tp,env))
             if (toExpr(p) == b)
                xc
             else q"$xc.map{ case $pc => $bc }"
        case flatMap(Lambda(p,b),x)
          => val pc = code(p)
             val (_,tp,xc) = typedCode(x,env,codeGen)
             val bc = codeGen(b,add(p,tp,env))
             if (irrefutable(p))
                q"$xc.flatMap{ case $pc => $bc }"
             else q"$xc.flatMap{ case $pc => $bc; case _ => Nil }"
        case groupBy(x)
          => val xc = codeGen(x,env)
             q"core.distributed.groupBy($xc)"
        case orderBy(x)
          => val xc = codeGen(x,env)
             q"core.distributed.groupBy($xc)"
        case coGroup(x,y)
          => val xc = codeGen(x,env)
             val yc = codeGen(y,env)
             q"core.distributed.coGroup($xc,$yc)"
        case cross(x,y)
          => val xc = codeGen(x,env)
             val yc = codeGen(y,env)
             q"core.distributed.cross($xc,$yc)"
        case reduce(BaseMonoid("+"),
                    flatMap(Lambda(p,Elem(LongConst(1))),x))
          => val xc = codeGen(x,env)
             q"$xc.size"
        case reduce(m,x)
          => val (_,tp,xc) = typedCode(x,env,codeGen)
             val fm = accumulator(m,tp,e)
             monoid(c,m) match {
               case Some(mc)
                 => if (isDistributed(x))
                      q"$xc.fold[$tp]($mc)($fm)"
                    else q"$xc.foldLeft[$tp]($mc)($fm)"
               case _ => q"$xc.reduce($fm)"
             }
        case _ => super.codeGen(e,env,codeGen)
    }

  /** Convert method calls to algebraic terms so that they can be optimized */
  override def algebraGen ( e: Expr ): Expr = e
}
