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
import scala.language.implicitConversions


package object diql {
  import core._
  import Parser.{parse,parseMany,parseMacro}

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

  def q_impl ( c: Context ) ( query: c.Expr[String] ): c.Expr[Any] = {
    import c.universe._
    val Literal(Constant(s:String)) = query.tree
    val cg = new { val context: c.type = c } with QueryCodeGenerator
    cg.code_generator(parse(s),s)
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
    val cg = new { val context: c.type = c } with QueryCodeGenerator
    val Literal(Constant(s:String)) = query.tree
    val lines = s.split("\n").toList
    val el = parseMany(s)
    val ec = ( for { i <- Range(0,el.length-1)
                   } yield cg.code_generator(el(i),(i+1)+") "+subquery(lines,el(i).pos,el(i+1).pos))
             ).toList :+ cg.code_generator(el.last,el.length+") "+subquery(lines,el.last.pos,null))
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

  def m_impl ( c: Context ) ( macroDef: c.Expr[String] ): c.Expr[Unit] = {
    import c.universe._
    val Literal(Constant(s:String)) = macroDef.tree
    parseMacro(s) match { case (nm,vars,e) => macro_defs += ((nm,(vars,e))) }
    c.Expr[Unit](q"()")
  }

  /** macro declaration */
  def m ( macroDef: String ): Unit = macro m_impl

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
