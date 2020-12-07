/*
 * Copyright © 2020 University of Texas at Arlington
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

package edu.uta.sql

import edu.uta.diql.core
import core.diql_explain
import edu.uta.diablo._
import edu.uta.diablo.Typechecker._
import core.Pretty.{print => pretty}


object SqlGen {
  def typecheck_code ( c: Code[Expr], env: Environment ) {
    c match {
      case Assignment(v,e)
      => typecheck(e,env)
      case CodeC(e)
      => ;
      case WhileLoop(p,b)
      => typecheck_code(b,env)
      case CodeBlock(l)
      => l.foreach(x => typecheck_code(x,env))
    }
  }

  def tree(t: Code[sql.Expr]): sql.Expr = {
    t match {
      case CodeBlock(e) => sql.listExp(e.map(tree))
      case Assignment(v, qs) =>
        qs match {
          case sql.Call(_, List(_, _, q)) => sql.Assignment(v, q)
          case  sql.Call(_,List(_, q)) => sql.Assignment(v, q)
          case sql.Elem(sql.MethodCall(n, o, List(l))) =>
            l match {
              case sql.Query(s,f,w,g) => sql.Assignment(v, sql.Query(s,f,w,g))
              case sql.MethodCall(n2, o2, l2) =>
               // l match {
                    sql.Assignment(v, sql.MethodCall(n, o, List(sql.MethodCall(n2, o2, l2))) )
                //  case _ => sql.Empty()
               // }
              case sql.Var(x) => sql.Assignment(v, sql.MethodCall(n, o, List(l)) )
              case _ => sql.Empty()
            }

          case sql.Elem(sql.Elem(q)) => sql.Assignment(v, q)

          case _ => null
        }

      case CodeC(sql.Elem(sql.Elem(c))) =>{
        c match{
          case sql.Call(n, List(a)) =>
            a match{
              case sql.Var(v) => sql.Call(n,List(sql.Var(v)))
              case sql.Project(sql.Var(x), c2) => sql.Call(n, List(sql.Project(sql.Var(x), c2)))
              case _ => sql.Empty()
            }
          case sql.MethodCall(sql.Var(x),foreach,List(sql.Var(p))) => sql.Call(p,List(sql.Var(x)))
          case _ => sql.Empty()
        }

     // sql.Empty()
      }
      case _ => null
    }
  }

  def translate_query ( query: String ): sql.Expr = {
    import AST.map
    val e = Parser.parse(query)
    if (diql_explain)
      println("Program:\n"+pretty(e.toString))
    typecheck(e)
    val e1 = Translator.translate(e)
    val ne = map(e1,Normalizer.normalizeAll)
    if (diql_explain)
      println("Comprehension term:\n"+pretty(ne.toString))
    val env: Environment = Translator.global_variables++Translator.external_variables
    typecheck_code(ne,env)
    val oe = map(ne,Optimizer.optimizeAll)
    if (diql_explain)
      println("Optimized comprehension term:\n"+pretty(oe.toString))
    val qt = map(oe,(x:Expr) => SqlTranslator.translate(x))
    val qs = tree(qt)
    if (diql_explain)
    println("SQL Tree:\n"+pretty(pretty(qs.toString)))
    qs
  }

  def main ( args: Array[String] ) {
    diql_explain = true
    translate_query(scala.io.Source.fromFile(args(0)).mkString)
  }
}