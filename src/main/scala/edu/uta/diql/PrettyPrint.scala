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
package edu.uta.diql.core

import scala.util.parsing.combinator.RegexParsers
import scala.util.Try


sealed abstract class Tree
    case class Node ( name: String, children: List[Tree] ) extends Tree
    case class TupleNode ( children: List[Tree] ) extends Tree
    case class MapNode ( binds: List[(Tree,Tree)] ) extends Tree
    case class LeafS ( value: String ) extends Tree
    case class LeafL ( value: Long ) extends Tree
    case class LeafD ( value: Double ) extends Tree


/** Pretty printer for case classes */
object Pretty extends RegexParsers {

  val screen_size = 80
  var prefix = ""

  val ident: Parser[String] = """[_a-zA-Z][_\$\w]*""".r
  val value: Parser[String] = """[^,\)]+""".r
  val string: Parser[String] = """"[^"]*"""".r

  def tree: Parser[Tree]
      = ( "Record" ~ "(" ~ "Map" ~ "(" ~ repsep( ident ~ "->" ~ tree, "," ) ~ ")" ~ ")"
            ^^ { case _~_~_~_~as~_~_ => MapNode(as.map{ case v~_~a => (LeafS(v),a) }) }
          | "RecordType" ~ "(" ~ "Map" ~ "(" ~ repsep( ident ~ "->" ~ tree, "," ) ~ ")" ~ ")"
            ^^ { case _~_~_~_~as~_~_ => MapNode(as.map{ case v~_~a => (LeafS(v),a) }) }
          | ident ~ "(" ~ repsep( tree, "," ) ~ ")" ^^ { case f~_~as~_ => Node(f,as) }
          | "(" ~ repsep( tree, "," ) ~ ")" ^^ { case _~as~_ => TupleNode(as) }
          | "None" ^^ { _ => Node("None",List()) }
          | string ^^ { LeafS(_) }
          | value ^^ { v => Try(v.toInt).toOption match {
                                            case Some(i) => LeafL(i)
                                            case _ => Try(v.toDouble).toOption match {
                                                        case Some(d) => LeafD(d)
                                                        case _ => LeafS(v)
                                                      }
                                }
                     }
        )

  def size ( e: Tree ): Int = {
    e match {
      case Node(f,as)
        => as.map(size(_)+1).sum + f.length + 2
      case TupleNode(as)
        => as.map(size(_)+1).sum + 4
      case MapNode(as)
        => as.map{ case (v,a) => size(v)+size(a)+4 }.sum + 4
      case LeafS(v)
        => v.length+2
      case LeafD(d)
        => d.toString.length
      case LeafL(i)
        => i.toString.length
    }
  }

  /** pretty-print trees */
  def pretty ( e: Tree, position: Int ): String = {
    e match {
      case Node(f,l) if position+size(e) <= screen_size
        => l.map(pretty(_,position)).mkString(f+"(", ", ", ")")
      case Node(f,l)
        => l.map(pretty(_,position+f.length+1))
            .mkString(f+"(", ",\n"+prefix+" "*(position+f.length+1), ")")
      case TupleNode(l)
        => l.map(pretty(_,position)).mkString("( ", ", ", " )")
      case MapNode(l) if position+size(e) <= screen_size
        => l.map{ case (v,a) => pretty(v,position)+" = "+pretty(a,position) }.mkString("< ", ", ", " >")
      case MapNode(l)
        => l.map{ case (v,a) => pretty(v,position+2)+" = "+pretty(a,position+size(v)+3) }
            .mkString("< ", ",\n"+prefix+" "*(position+2), " >")
      case LeafS(v)
        => v.toString
      case LeafD(d)
        => d.toString
      case LeafL(i)
        => i.toString
    }
  }

 /** pretty-print the printout of a case class object */
  def print ( s: String ): String = {
    parseAll(tree,s) match {
      case Success(t,_) => pretty(t,0)
      case e => println(e); s
    }
  }

  def print ( s: String, prfx: String ): String = {
    prefix = prfx
    parseAll(tree,s) match {
      case Success(t,_) => prefix+pretty(t,0)
      case _ => s
    }
  }
}
