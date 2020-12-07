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

object sql {
  import scala.util.parsing.input.Positional

  sealed abstract class Expr ( var tpe: Any = null ) extends Positional
  case class Query(projection: Select, from: Expr, selection: Option[Expr], groupBy: Option[GroupBy] ) extends Expr
  case class Select ( attr: List[Attr]) extends Expr
  case class Attr (cols: Expr, aggcol: Option[List[Expr]], agg:Option[List[Expr]]) extends Expr
  case class Column(cols: List[Expr]) extends Expr
  case class From(join: List[Expr]) extends Expr
  case class Join(table1: String, table2: String, condition: Expr) extends Expr
  case class Where(c: List[Expr]) extends Expr
  case class Conj(c: List[Expr], name: String, cn: List[Expr]) extends Expr
  case class Cond(left: Expr, op: String, right: Expr) extends Expr
  case class GroupBy(attr: List[Expr]) extends Expr
  case class reduce (input: Expr ) extends Expr
  case class MethodCall ( obj: Expr, method: String, args: List[Expr] ) extends Expr
  case class Filter(left: Expr, operand: Expr, right: Expr) extends Expr
  case class Call( name: String, args: List[Expr] ) extends Expr
  case class StringConst ( value: String ) extends Expr {
    override def toString: String = "StringConst(\""+value+"\")"
  }
  case class IntConst ( value: Int ) extends Expr
  case class DoubleConst ( value: Double ) extends Expr
  case class BoolConst ( value: Boolean ) extends Expr
  case class Empty () extends Expr
  case class print(name: String) extends Expr
  case class listExp(name: List[Expr]) extends Expr
  case class Init ( args: List[Expr] ) extends Expr
  case class Var ( name: String ) extends Expr
  case class Elem ( elem: Expr ) extends Expr
  case class Compile ( compile: Map[String,Expr] ) extends Expr
  case class Nth ( tuple: Expr, num: Int ) extends Expr
  case class Tuple ( components: List[Expr] ) extends Expr
  case class Project(c1: Expr, c2: String) extends Expr
  case class Assignment(v: String, query: Expr) extends Expr
  case class CodeC(v:Expr) extends Expr
}
