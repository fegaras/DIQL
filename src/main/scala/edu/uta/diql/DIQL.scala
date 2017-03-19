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
package edu.uta

import scala.reflect.macros.whitebox.Context
import scala.language.experimental.macros
import scala.util.parsing.input.Position
import java.io._
import scala.language.implicitConversions

package object diql {
  import core._
  import Normalizer.normalizeAll
  import Translator.translate
  import Optimizer.optimizeAll
  import Pretty.{print=>pretty_print}
  import Parser.{parse,parseMany}
  import CodeGeneration.typecheck

  /** Used for inverse ordering */
  case class Inv[K] ( value: K )

  implicit def inv2ordered[K] ( x: Inv[K] ) ( implicit ord: K => Ordered[K] ): Ordered[Inv[K]]
    = new Ordered[Inv[K]] {
        def compare ( y: Inv[K] ): Int = -x.value.compare(y.value)
  }

  /** Used by the avg/e aggregation */
  case class Avg[T] ( val sum: T, val count: Long ) ( implicit num: Numeric[T] ) {
    def avg_combine ( other: Avg[T] ): Avg[T]
       = new Avg[T](num.plus(sum,other.sum),count+other.count)
    def value = num.toDouble(sum)/count
  }

  private def code_generator ( c: Context ) ( query: Expr, query_text: String ): c.Expr[Any] = {
    import c.universe.{Expr=>_,_}
    try {
      if (debug_diql)
         println("\nQuery:\n"+query_text)
      // val e = normalizeAll(distributed.algebraGen(translate(query)))  // algebraGen needs more work
      val e = normalizeAll(translate(query))
      if (debug_diql)
         println("Algebraic term:\n"+pretty_print(e.toString))
      typecheck(c)(e)
      val oe = normalizeAll(optimizeAll(c)(e))
      if (debug_diql)
         println("Optimized term:\n"+pretty_print(oe.toString))
      val tp = typecheck(c)(oe)
      val ec = distributed.codeGen(c)(oe,Map())
      if (debug_diql)
         println("Scala code:\n"+showCode(ec))
      if (debug_diql)
         println("Scala type: "+showCode(tp))
      c.Expr[Any](ec)
    } catch {
      case ex: Any
        => println(ex)
        if (debug_diql) {
           val sw = new StringWriter
           ex.printStackTrace(new PrintWriter(sw))
           println(sw.toString)
        }
        c.Expr[Any](q"()")
    }
  }

  def q_impl ( c: Context ) ( query: c.Expr[String] ): c.Expr[Any] = {
    import c.universe._
    val Literal(Constant(s:String)) = query.tree
    code_generator(c)(parse(s),s)
  }

  /** translate the query to Scala code */
  def q ( query: String ): Any = macro q_impl

  private def subquery ( lines: List[String], from: Position, to: Position ): String = {
    val c = (if (to == null) lines.drop(from.line-1) else lines.take(to.line).drop(from.line-1)).toArray
    c(0) = c(0).substring(from.column-1)
    if (to != null)
       c(c.length-1) = c(c.length-1).substring(0,from.column-1)
    c.mkString("\n")
  }

  def qs_impl ( c: Context ) ( query: c.Expr[String] ): c.Expr[List[Any]] = {
    import c.universe._
    val Literal(Constant(s:String)) = query.tree
    val lines = s.split("\n").toList
    val el = parseMany(s)
    val ec = ( for { i <- Range(0,el.length-1)
                   } yield code_generator(c)(el(i),(i+1)+") "+subquery(lines,el(i).pos,el(i+1).pos))
             ).toList :+ code_generator(c)(el.last,el.length+") "+subquery(lines,el.last.pos,null))
    c.Expr[List[Any]](q"List(..$ec)")
  }

  /** compile many DIQL queries to Scala code that returns List[Any] */
  def qs ( query: String ): List[Any] = macro qs_impl

  def debug_impl ( c: Context ) ( b: c.Expr[Boolean] ): c.Expr[Unit] = {
    import c.universe._
    b.tree match {
      case Literal(Constant(bv:Boolean))
        => debug_diql = bv
      case _ => ;
    }
    c.Expr[Unit](q"()")
  }

  /** turn on/off debugging mode */
  def debug ( b: Boolean ): Unit = macro debug_impl

  def monoid_impl ( c: Context ) ( monoid: c.Expr[String], zero: c.Expr[Any] ): c.Expr[Unit] = {
    import c.universe._
    monoid.tree match {
      case Literal(Constant(m:String))
        => if (monoids.contains(m))
              monoids = monoids-m
           if (showCode(zero.tree) == "null")
              monoids = monoids+((m,null))
           else monoids = monoids+((m,showCode(zero.tree)))
      case _ => ;
    }
    c.Expr[Unit](q"()")
  }

  /** specify a new monoid */
  def monoid ( monoid: String, zero: Any ): Unit = macro monoid_impl
}
