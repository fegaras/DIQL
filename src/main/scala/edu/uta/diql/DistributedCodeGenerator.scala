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

import scala.reflect.ClassTag
import scala.reflect.macros.whitebox.Context
import scala.language.higherKinds


/** Distributed frameworks, such as Spark and Flink, must implement this class */
abstract class DistributedCodeGenerator[DataBag[_]] extends CodeGeneration {
  import c.universe.{Expr=>_,_}

  /** The code generator for algebraic terms */
  def codeGen ( e: Expr, env: Map[c.Tree,c.Tree] ): c.Tree

  /** Convert DataBag method calls to algebraic terms so that they can be optimized */
  def algebraGen ( e: Expr ): Expr

  /** the actual type of the DataBag */
  def typeof ( c: Context ): c.Type

  /** construct a type instance of the DataBag */
  def mkType ( c: Context ) ( tp: c.Tree ): c.Tree

  def flatMap[A,B] ( f: (A) => TraversableOnce[B], S: DataBag[A] )
                   (implicit tag: ClassTag[B]): DataBag[B]

  def flatMap2[A,B] ( f: (A) => DataBag[B], S: DataBag[A] )
                    (implicit tag: ClassTag[B]): DataBag[B]

  def flatMap[A,B] ( f: (A) => DataBag[B], S: Traversable[A] ): DataBag[B]

  def groupBy[K,A] ( S: DataBag[(K,A)] )
                   (implicit kt: ClassTag[K], vt: ClassTag[A]): DataBag[(K,Iterable[A])]

  def orderBy[K,A] ( S: DataBag[(K,A)] )
                   ( implicit ord: Ordering[K], kt: ClassTag[K], at: ClassTag[A] ): DataBag[A]

  def reduce[A] ( acc: (A,A) => A, S: DataBag[A] ): A

  def coGroup[K,A,B] ( X: DataBag[(K,A)], Y: DataBag[(K,B)] )
                     (implicit kt: ClassTag[K], vt: ClassTag[A]): DataBag[(K,(Iterable[A],Iterable[B]))]

  def coGroup[K,A,B] ( X: Traversable[(K,A)], Y: DataBag[(K,B)] )
                     (implicit kt: ClassTag[K], vt: ClassTag[B]): DataBag[(K,(Iterable[A],Iterable[B]))]

  def coGroup[K,A,B] ( X: DataBag[(K,A)], Y: Traversable[(K,B)] )
                     (implicit kt: ClassTag[K], vt: ClassTag[A]): DataBag[(K,(Iterable[A],Iterable[B]))]

  def cross[A,B] ( X: DataBag[A], Y: DataBag[B] )
                 (implicit bt: ClassTag[B]): DataBag[(A,B)]

  def cross[A,B] ( X: Traversable[A], Y: DataBag[B] )
                 (implicit bt: ClassTag[B]): DataBag[(A,B)]

  def cross[A,B] ( X: DataBag[A], Y: Traversable[B] )
                 (implicit bt: ClassTag[A]): DataBag[(A,B)]

  def merge[A] ( X: DataBag[A], Y: DataBag[A] ): DataBag[A]

  def collect[A] ( X: DataBag[A] ): Array[A]

  def cache[A] ( X: DataBag[A] ): DataBag[A]

  def head[A] ( X: DataBag[A] ): A
}
