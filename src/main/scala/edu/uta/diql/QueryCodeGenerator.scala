package edu.uta.diql.core

import scala.reflect.macros.whitebox.Context
import java.io._


abstract class QueryCodeGenerator {
  val context: Context

  /** Translate a DIQL query to Scala byte code */
  def code_generator ( query: Expr, query_text: String, line: Int ): context.Expr[Any] = {
    import context.universe.{Expr=>_,_}
    import Normalizer.normalizeAll
    import Pretty.{print=>pretty_print}
    try {
      if (debug_diql)
         println("\nQuery:\n"+query_text)
      val cg = new { val c: context.type = context } with SparkCodeGenerator
      val optimizer = new { val c: context.type = context } with Optimizer
      val translator = new { val c: context.type = context } with Translator
      cg.line = line
      distributed = cg
      // val e = normalizeAll(distributed.algebraGen(translate(query)))  // algebraGen needs more work
      val e = normalizeAll(translator.translate(query))
      if (debug_diql)
         println("Algebraic term:\n"+pretty_print(e.toString))
      cg.typecheck(e)
      val oe = normalizeAll(optimizer.optimizeAll(e))
      if (debug_diql)
         println("Optimized term:\n"+pretty_print(oe.toString))
      val tp = cg.typecheck(oe)
      val ec = cg.codeGen(oe,Map())
      if (debug_diql)
         println("Scala code:\n"+showCode(ec))
      if (debug_diql)
         println("Scala type: "+showCode(tp))
      context.Expr[Any](ec)
    } catch {
      case ex: Any
        => println(ex)
        if (debug_diql) {
           val sw = new StringWriter
           ex.printStackTrace(new PrintWriter(sw))
           println(sw.toString)
        }
        context.Expr[Any](q"()")
    }
  }  
}
