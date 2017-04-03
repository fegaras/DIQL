package edu.uta.diql

import scala.reflect.macros.whitebox.Context
import scala.collection.mutable.HashMap


package object core {

  var distributed: SparkCodeGenerator = new { val c = null } with SparkCodeGenerator

  /** list of defined monoids; other infix operations are just semigroups */
  var monoids
      = Map( "+" -> "0", "*" -> "1", "&&" -> "true", "||" -> "false",
             "count" -> "0", "avg_combine" -> "Avg(0,0L)", "min" -> null,
             "max" -> null, "avg" -> null
           )

  type macroDefType = (List[(String,Type)],Expr)

  /** macro definitions */
  val macro_defs = new HashMap[String,macroDefType]()

  
  def findMacros ( name: String, args: Int ): Iterable[macroDefType]
    = macro_defs.filter{ case (n,(ps,b)) => n == name && ps.length == args }.values

  /** return the zero element of the monoid, if any */
  def monoid ( c: Context, m: String ): Option[c.Tree] = {
    import c.universe._
    if (monoids.contains(m) && monoids(m) != null)
       Some(c.parse(monoids(m)))
    else None
  }

  var debug_diql = false
}
