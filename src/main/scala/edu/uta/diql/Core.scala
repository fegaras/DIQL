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
package edu.uta.diql

import scala.reflect.macros.whitebox.Context
import scala.collection.mutable.HashMap


package object core {

  var distributed = core.DistributedEvaluator.distributed

  /** list of defined monoids; other infix operations are just semigroups */
  var monoids
      = Map( "+" -> "0", "*" -> "1", "&&" -> "true", "||" -> "false",
             "count" -> "0", "avg_combine" -> "new Avg(0,0L)", "min" -> null,
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

  var diql_explain = false
}
