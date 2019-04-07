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

import scala.reflect.macros.whitebox.Context
import java.io._
import scala.collection.immutable.HashMap


object DistributedEvaluator {
  var distributed: SparkCodeGenerator = new { val c = null } with SparkCodeGenerator
}

abstract class QueryCodeGenerator {
  val context: Context

  val cg = new { val c: context.type = context } with SparkCodeGenerator
  val sg = new { val c: context.type = context } with Streaming
  val optimizer = new { val c: context.type = context } with Optimizer

  /** Translate a DIQL query to Scala byte code */
  def code_generator ( e: Expr, query_text: String, line: Int, debug: Boolean,
                       env: cg.Environment = Map() ): context.Expr[Any] = {
    import context.universe.{Expr=>_,_}
    import Normalizer.normalizeAll
    import Pretty.{print=>pretty_print}
    try {
      cg.line = line
      distributed = cg
      cg.typecheck(e,env)
      val oe = normalizeAll(optimizer.optimizeAll(e,env))
      if (diql_explain)
        println("Optimized term:\n"+pretty_print(oe.toString))
      cg.typecheck(oe,env)
      if (diql_streaming) {
        val se = sg.findHomomorphisms(oe,new HashMap())
        println("Streaming term:\n"+pretty_print(se.toString))
      }
      val de = if (debug)
                  normalizeAll(Call("debug",
                                    List(Provenance.embedLineage(oe,cg.isDistributed(_)),
                                         BoolConst(cg.isDistributed(oe)),
                                         Call("List",Provenance.exprs.map(StringConst(_))))))
               else oe
      if (debug && diql_explain)
         println("Debugging term:\n"+pretty_print(de.toString))
      val ec = cg.codeGen(de,env)
      if (diql_explain)
         println("Scala code:\n"+showCode(ec))
      val tp = cg.getType(ec,env)
      if (diql_explain)
         println("Scala type: "+showCode(tp))
      context.Expr[Any](ec)
    } catch {
      case ex: Any
        => println(ex)
           if (diql_explain) {
              val sw = new StringWriter
              ex.printStackTrace(new PrintWriter(sw))
              println(sw.toString)
           }
           context.Expr[Any](q"()")
    }
  }  
}
