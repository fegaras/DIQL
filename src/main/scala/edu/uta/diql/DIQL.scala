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
import java.io.Serializable


package object diql {
  import core._
  import Parser.{parse,parseMany,parseMacros}

  /** Used for inverse ordering (Flink requires POJOs for custom key ordering) */
  @SerialVersionUID(100L)
  class Inv[K] ( val value: K ) ( implicit ord: K => Ordered[K] )
          extends Comparable[Inv[K]] with Serializable {
    override def compareTo ( y: Inv[K] ): Int = -value.compare(y.value)
    override def toString: String = "Inv("+value+")"
  }

  /** Used by the avg/e aggregation */
  @SerialVersionUID(100L)
  class Avg[T] ( val sum: T, val count: Long ) ( implicit num: Numeric[T] ) extends Serializable {
    def avg_combine ( other: Avg[T] ): Avg[T]
       = new Avg[T](num.plus(sum,other.sum),count+other.count)
    def value = num.toDouble(sum)/count
  }

  private var tab_count = -3
  private var trace_count = 0L

  /** evaluate a DIQL expression and print tracing info */
  def trace[T] ( msg: String, value: => T ): T = {
    tab_count += 3
    trace_count += 1
    val c = trace_count
    println(" "*tab_count+"*** "+c+": "+msg)
    val v = value
    print(" "*tab_count+"--> "+c+": ")
    println(v)
    tab_count -= 3
    v
  }

  @SerialVersionUID(100L)
  sealed abstract class Lineage ( val tree: Int, val value: Any ) extends Serializable
    case class UnaryLineage ( override val tree: Int, override val value: Any,
                              lineage: Traversable[Lineage] )
         extends Lineage(tree,value)
    case class BinaryLineage ( override val tree: Int, override val value: Any,
                               left: Traversable[Lineage], right: Traversable[Lineage] )
         extends Lineage(tree,value)

  def debug[T] ( value: (T,Lineage) ): T = value._1

  def debug[T] ( value: Iterable[(T,Lineage)] ): Iterable[T] = value.map(_._1)

  def q_impl ( c: Context ) ( query: c.Expr[String] ): c.Expr[Any] = {
    import c.universe._
    val Literal(Constant(s:String)) = query.tree
    val cg = new { val context: c.type = c } with QueryCodeGenerator
    cg.code_generator(parse(s),s,query.tree.pos.line,false)
  }

  /** translate the query to Scala code */
  def q ( query: String ): Any = macro q_impl

  def debug_impl ( c: Context ) ( query: c.Expr[String] ): c.Expr[Any] = {
    import c.universe._
    val Literal(Constant(s:String)) = query.tree
    val cg = new { val context: c.type = c } with QueryCodeGenerator
    cg.code_generator(parse(s),s,query.tree.pos.line,true)
  }

  /** translate the query to Scala code that debugs the query */
  def debug ( query: String ): Any = macro debug_impl

  private def subquery ( lines: List[String], from: Position, to: Position ): String = {
    val c = (if (to == null) lines.drop(from.line-1) else lines.take(to.line).drop(from.line-1)).toArray
    c(0) = c(0).substring(from.column-1)
    if (to != null)
       c(c.length-1) = c(c.length-1).substring(0,to.column-1)
    c.mkString("\n").split(';')(0)
  }

  private def start ( lines: List[String], from: Position ): Int
    = lines.take(from.line-1).map(_.length()).reduce(_+_)

  def qs_impl ( c: Context ) ( query: c.Expr[String] ): c.Expr[List[Any]] = {
    import c.universe._
    val cg = new { val context: c.type = c } with QueryCodeGenerator
    val Literal(Constant(s:String)) = query.tree
    val lines = s.split("\n").toList
    val el = parseMany(s)
    val ec = ( for { i <- Range(0,el.length-1)
                   } yield cg.code_generator(el(i),
                               (i+1)+") "+subquery(lines,el(i).pos,el(i+1).pos),
                               query.tree.pos.line+el(i).pos.line-1,false)
             ).toList :+ cg.code_generator(el.last,
                               el.length+") "+subquery(lines,el.last.pos,null),
                               query.tree.pos.line+el.last.pos.line-1,false)
    c.Expr[List[Any]](q"List(..$ec)")
  }

  /** compile many DIQL queries to Scala code that returns List[Any] */
  def qs ( query: String ): List[Any] = macro qs_impl

  def explain_impl ( c: Context ) ( b: c.Expr[Boolean] ): c.Expr[Unit] = {
    import c.universe._
    b.tree match {
      case Literal(Constant(bv:Boolean))
        => diql_explain = bv
      case _ => ;
    }
    c.Expr[Unit](q"()")
  }

  /** turn on/off compilation tracing mode */
  def explain ( b: Boolean ): Unit = macro explain_impl
                               
  def m_impl ( c: Context ) ( macroDef: c.Expr[String] ): c.Expr[Unit] = {
    import c.universe._
    val qcg = new { val context: c.type = c } with QueryCodeGenerator
    val Literal(Constant(s:String)) = macroDef.tree
    val lines = s.split("\n").toList
    val el = parseMacros(s)
    def macro_def ( i: Int )
      = el(i) match {
          case (nm,vars,e)
            => val env = vars.map{ case (n,tp) => (qcg.cg.code(VarPat(n)),qcg.cg.Type2Tree(tp)) }.toMap
               val npos = if (i+1>=el.length) null else el(i+1)._3.pos
               val ec = qcg.code_generator(e,
                               (i+1)+") "+subquery(lines,e.pos,npos),
                               macroDef.tree.pos.line+e.pos.line-1,
                               false,env)
               macro_defs += ((nm,(vars,e)))
        }
    for { i <- 0 to el.length-1 } macro_def(i)
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
