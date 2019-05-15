/*
 * Copyright © 2019 University of Texas at Arlington
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
package edu.uta.diablo
import edu.uta.diql.core
import core.Pretty.{print=>pretty}
import core.diql_explain


object Diablo {
  def translate_query ( query: String ): (Code[core.Expr],Map[String,core.Type]) = {
    import Typechecker._
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
    map(ne,(x:Expr) => typecheck(x,env))
    val oe = map(ne,Optimizer.optimizeAll)
    val noe = map(oe,Normalizer.normalizeAll)
    if (diql_explain)
       println("Optimized comprehension term:\n"+pretty(noe.toString))
    map(oe,(x:Expr) => typecheck(x,env))
    val ae = map(oe,(x:Expr) => ComprehensionTranslator.translate(x))
    if (diql_explain)
       println("Algebraic term:\n"+pretty(ae.toString))
    val nae = map(ae,core.Normalizer.normalizeAll)
    ( nae, Translator.global_variables.mapValues(ComprehensionTranslator.translate) )
  }

  def main ( args: Array[String] ) {
    diql_explain = true
    translate_query(scala.io.Source.fromFile(args(0)).mkString)
  }
}
