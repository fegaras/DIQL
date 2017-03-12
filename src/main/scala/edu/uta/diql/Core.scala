package edu.uta.diql

import scala.reflect.macros.whitebox.Context

package object core {

  /** Distributed frameworks, such as Spark and Flink, must implement this trait */
  trait DistributedCodeGenerator {
    def codeGen ( c: Context ) ( e: Expr, env: Map[c.Tree,c.Tree] ): c.Tree
  }

  val distr = SparkCodeGenerator

  /** list of defined monoids; other infix operations are just semigroups */
  var monoids
      = Map( "+" -> "0", "*" -> "1", "&&" -> "true", "||" -> "false",
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

  var debug_diql = false
  }
  