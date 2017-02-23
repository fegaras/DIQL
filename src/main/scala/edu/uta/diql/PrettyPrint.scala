package edu.uta.diql

import scala.util.parsing.combinator.RegexParsers
import scala.util.Try


sealed abstract class Tree
    case class Node ( name: String, children: List[Tree] ) extends Tree
    case class LeafS ( value: String ) extends Tree
    case class LeafL ( value: Long ) extends Tree
    case class LeafD ( value: Double ) extends Tree


/** Pretty printer for case classes */
object Pretty extends RegexParsers {

  val screen_size = 80
  var prefix = ""

  val charMaps = Map("\"" -> "\\\\\"", "\n" -> "\\\\n", "\t" -> "\\\\t", "\r" -> "\\\\r")

  def coerce(s: String) = charMaps.foldLeft(s){ case (s,p) => s.replaceAll(p._1,p._2) }

  val ident = """[_\p{L}][_\p{L}\p{Nd}]*""".r
  val value = """[^\,\)]+""".r

  def tree: Parser[Tree]
      = ( ident ~ "(" ~ repsep( tree, "," ) ~ ")" ^^ { case f~_~as~_ => Node(f,as) }
          | "None" ^^ { _ => Node("None",List()) }
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
      case Node(f,l) if (position+size(e) <= screen_size)
        => l.map(pretty(_,position)).mkString(f+"(", ", ", ")")
      case Node(f,l)
        => l.map(pretty(_,position+f.length+1))
            .mkString(f+"(", ",\n"+prefix+" "*(position+f.length+1), ")")
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
      case e => s
    }
  }

  def print ( s: String, prfx: String ): String = {
    prefix = prfx
    parseAll(tree,s) match {
      case Success(t,_) => prefix+pretty(t,0)
      case e => s
    }
  }
}
