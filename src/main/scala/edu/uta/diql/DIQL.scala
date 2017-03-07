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

package object diql {
  import CodeGeneration._
  import SparkCodeGenerator._
  import edu.uta.diql.{ Expr => E, _ }

  /** Distributed frameworks, such as Spark and Flink, must implement this trait */
  trait DistributedCodeGenerator {
    def codeGen ( c: Context ) ( e: E, env: Map[c.Tree,c.Tree] ): c.Tree
  }

  val distr = edu.uta.diql.SparkCodeGenerator

  /** list of defined monoids; other infix operations are just semigroups*/
  var monoids = Map( "+" -> "0", "*" -> "1", "&&" -> "true", "||" -> "false",
                     "count" -> "0", "avg_combine" -> "Avg(0,0L)", "min" -> null,
                     "max" -> null, "avg" -> null
                   )

  /** return the zero element of the monoid, if any */
  def monoid ( c: Context, m: String ): Option[c.Tree] = {
    import c.universe._
    if (monoids.contains(m) && monoids(m) != null)
       Some(c.parse(monoids(m)))
    else None
  }

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

  /** Typecheck the query using the Scala's typechecker */
  def typecheck ( c: Context ) ( query: E ): c.Tree = {
    def rec ( c: Context ) ( e: E, env: Map[c.Tree,c.Tree] ): c.Tree
        = code(c)(e,env,rec(c)(_,_))
    getType(c)(code(c)(query,Map(),rec(c)(_,_)),Map())
  }

  var debug = false

  def code_generator ( c: Context ) ( query: E, query_text: String ): c.Expr[Any] = {
    import c.universe._
    try {
      if (debug)
         println("\nQuery:\n"+query_text)
      val e = Normalizer.normalizeAll(Translator.translate(query))
      if (debug)
         println("Algebraic term:\n"+Pretty.print(e.toString))
      typecheck(c)(e)
      val oe = Normalizer.normalizeAll(Optimizer.optimizeAll(c)(e))
      if (debug)
         println("Optimized term:\n"+Pretty.print(oe.toString))
      val tp = typecheck(c)(oe)
      val ec = distr.codeGen(c)(oe,Map())
      if (debug)
         println("Scala code:\n"+showCode(ec))
      if (debug)
         println("Scala type: "+showCode(tp))
      c.Expr[Any](ec)
    } catch {
      case ex: Any => println(ex)
      if (debug) {
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
    code_generator(c)(Parser.parse(s),s)
  }

  def q ( query: String ): Any = macro q_impl

  def subquery ( lines: List[String], from: Position, to: Position ): String = {
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
    val el = Parser.parseMany(s)
    val ec = ( for { i <- Range(0,el.length-1)
                   } yield code_generator(c)(el(i),(i+1)+") "+subquery(lines,el(i).pos,el(i+1).pos))
             ).toList :+ code_generator(c)(el.last,el.length+") "+subquery(lines,el.last.pos,null))
    c.Expr[List[Any]](q"List(..$ec)")
  }

  def qs ( query: String ): List[Any] = macro qs_impl

  def debug_impl ( c: Context ) ( b: c.Expr[Boolean] ): c.Expr[Unit] = {
    import c.universe._
    b.tree match {
      case Literal(Constant(bv:Boolean))
        => debug = bv
      case _ => ;
    }
    c.Expr[Unit](q"()")
  }

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

  def monoid ( monoid: String, zero: Any ): Unit = macro monoid_impl
}
