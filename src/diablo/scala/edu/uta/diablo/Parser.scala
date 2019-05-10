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

import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.token.StdTokens
import scala.util.matching.Regex


trait MyTokens extends StdTokens {
  case class LongLit ( chars: String ) extends Token
  case class DoubleLit ( chars: String ) extends Token
  case class CharLit ( chars: String ) extends Token
  case class InfixOpr ( chars: String ) extends Token
}

class MyLexical extends StdLexical with MyTokens {
  /* a parser for regular expressions */
  def regex ( r: Regex ): Parser[String]
      = new Parser[String] {
            def apply ( in: Input )
                = r.findPrefixMatchOf(in.source.subSequence(in.offset,in.source.length)) match {
                        case Some(matched)
                          => Success(in.source.subSequence(in.offset,in.offset+matched.end).toString,
                                     in.drop(matched.end))
                        case None => Failure("string matching regex `"+r+"' expected but "+in.first+" found",in)
                  }
      }

  override def token: Parser[Token] = infixOpr | longLit | doubleLit | charLit | super.token

  /* long integers */
  def longLit: Parser[Token]
      = regex("""[0-9]+[Ll]""".r) ^^ { LongLit }

  /* floating point numbers */
  def doubleLit: Parser[Token]
      = regex("""[0-9]*[\.][0-9]+([eE][+-]?[0-9]+)?[FfDd]?""".r) ^^ { DoubleLit }

  /* character literal */
  def charLit: Parser[Token]
      = regex("""'[^']'""".r) ^^ { CharLit }

  /* an infix operator can be any sequence of special chars, except delimiters, etc */ 
  def infixOpr: Parser[Token]
      = regex("""[^\s\w\$\(\)\[\]\{\}\'\"\`\.\;\,\\/]+""".r) ^^
        { s => if (delimiters.contains(s)) Keyword(s) else InfixOpr(s) }
}

object Parser extends StandardTokenParsers {
  var queryText: String = ""

  override val lexical = new MyLexical

  lexical.delimiters += ( "(" , ")" , "[", "]", "{", "}", "," , ":", ";", ".", "=>", "=", "->",
                          "||", "&&", "!", "==", "<=", ">=", "<", ">", "!=", "+", "-", "*", "/", "%",
                          "^", "|", "&" )

  lexical.reserved += ( "var", "for", "in", "do", "while", "having", "if", "else", "true", "false", "external" )

  /* groups of infix operator precedence, from low to high */
  val operator_precedence: List[Parser[String]]
      = List( "||"|"|", "^", "&&"|"&", "<="|">="|"<"|">", "=="|"!=", "+"|"-", "*"|"/"|"%" )

  /* all infix operators not listed in operator_precedence have the same highest precedence */  
  val infixOpr: Parser[String]
      = accept("infix operator",{ case t: lexical.InfixOpr => t.chars })
  val precedence: List[Parser[String]]
      = operator_precedence :+ infixOpr
  val allInfixOpr: Parser[String]
      = operator_precedence.fold(infixOpr)(_|_)

  /* group infix operations into terms based on the operator precedence, from low to high */
  def terms ( level: Int ): Parser[(Expr,Expr)=>Expr]
      = precedence(level) ^^
        { op => (x:Expr,y:Expr) => Call(op,List(x,y)) }
  def infix ( level: Int ): Parser[Expr]
      = if (level >= precedence.length) factor
        else infix(level+1) * terms(level)

  def fromRaw ( s: String ): String = s.replaceAllLiterally("""\n""","\n")
        
  def expr: Parser[Expr]
      = infix(0) | factor

  def char: Parser[String]
      = accept("char literal",{ case t: lexical.CharLit => t.chars })

  def int: Parser[Int]
      = numericLit ^^ { _.toInt }

  def long: Parser[Long]
      = accept("long literal",{ case t: lexical.LongLit => t.chars.init.toLong })

  def double: Parser[Double]
      = accept("double literal",{ case t: lexical.DoubleLit => t.chars.toDouble })

 def factorList ( e: Expr ): Parser[Expr]
     = ( "[" ~ expr ~ opt( "," ~ expr ) ~ "]" ^^
         { case _~i~None~_ => VectorIndex(e,i)
           case _~i~Some(_~j)~_ => MatrixIndex(e,i,j) }
       | "." ~ ident ^^
         { case _~a => Project(e,a) }
       | "#" ~ int ^^
         { case _~n => Nth(e,n) }
       )

 def factor: Parser[Expr]
      = term ~ rep( factorList(IntConst(0)) ) ^^
        { case e~s => s.foldLeft(e){
                          case (r,Project(_,a)) => Project(r,a)
                          case (r,Nth(_,n)) => Nth(r,n)
                          case (r,VectorIndex(_,i)) => VectorIndex(r,i)
                          case (r,MatrixIndex(_,i,j)) => MatrixIndex(r,i,j)
                          case (r,_) => r } }

  def term: Parser[Expr]
      = ( ( "-" | "+" | "!" ) ~ expr ^^
          { case o~e => Call(o,List(e)) }
        | allInfixOpr ~ "/" ~ term ^^
          { case op~_~e => reduce(BaseMonoid(op),e) }
        | "if" ~ "(" ~ expr ~ ")" ~ expr ~ "else" ~ expr ^^
          { case _~_~p~_~t~_~e => IfE(p,t,e) }
        | "(" ~ repsep( expr, "," ) ~ ")" ^^
          { case _~es~_ => if (es.length==1) es.head else Tuple(es) }
        | "<" ~ rep1sep( ident ~ "=" ~ expr, "," ) ~ ">" ^^
          { case _~es~_ => Record(es.map{ case n~_~v => (n,v) }.toMap) }
        | "[" ~ repsep( expr, "," ) ~ "]" ^^
          { case _~es~_ => Collection("list",es) }
        | "{" ~ repsep( expr, "," ) ~ "}" ^^
          { case _~es~_ => Collection("bag",es) }
        | ident ~ "(" ~ repsep( expr, "," ) ~ ")" ^^
          { case "vector"~_~es~_ => Collection("vector",es)
            case "matrix"~_~es~_ => Collection("matrix",es)
            case f ~_~ps~_ => Call(f,ps) }
        | "true" ^^^ { BoolConst(true) }
        | "false" ^^^ { BoolConst(false) }
        | ident ^^
          { s => Var(s) }
        | double ^^
          { s => DoubleConst(s) }
        | long ^^
          { s => LongConst(s) }
        | int ^^
          { s => IntConst(s) }
        | stringLit ^^
          { s => StringConst(fromRaw(s)) }
        | char ^^
          { s => CharConst(fromRaw(s).apply(1)) }
        | failure("illegal start of expression")
        )

  def pat: Parser[Pattern]
      = ( "(" ~ repsep( pat, "," ) ~ ")"
          ^^ { case _~ps~_ => if (ps.length==1) ps.head else TuplePat(ps) }
        | "_"
          ^^^ { StarPat() }
        | ident
          ^^ { s => if (s == "_") StarPat() else VarPat(s) }
        | failure("illegal start of pattern")
        )

  def stmt: Parser[Stmt]
      = ( "var" ~ ident ~ ":" ~ stype ~ opt( "=" ~ expr ) ^^
          { case _~v~_~t~None => DeclareVar(v,t,None)
            case _~v~_~t~Some(_~e) => DeclareVar(v,t,Some(e)) }
        | "external" ~ ident ~ ":" ~ stype ^^
          { case _~v~_~t => DeclareExternal(v,t) }
        | "{" ~ rep( stmt ~ ";" ) ~ "}" ^^
          { case _~ss~_ => if (ss.length==1) ss.head match { case s~_ => s } else Block(ss.map{ case s~_ => s }) }
        | factor ~ "=" ~ expr ^?
          { case (d:Var)~_~e => Assign(d,e)
            case (d:Nth)~_~e => Assign(d,e)
            case (d:Project)~_~e => Assign(d,e)
            case (d:VectorIndex)~_~e => Assign(d,e)
            case (d:MatrixIndex)~_~e => Assign(d,e) }
        | "for" ~ ident ~ "=" ~ expr ~ "," ~ expr ~ opt( "," ~ expr ) ~ "do" ~ stmt ^^
          { case _~v~_~a~_~b~None~_~s => ForS(v,a,b,IntConst(1),s)
            case _~v~_~a~_~b~Some(_~c)~_~s => ForS(v,a,b,c,s) }
        | "for" ~ ident ~ "in" ~ expr ~ "do" ~ stmt ^^
          { case _~v~_~e~_~s => ForeachS(v,e,s) }
        | "while" ~ "(" ~ expr ~ ")" ~ stmt ^^
          { case _~_~p~_~s => WhileS(p,s) }
        | "if" ~ "(" ~ expr ~ ")" ~ stmt ~ opt( "else" ~ stmt ) ^^
          { case _~_~p~_~st~None => IfS(p,st,Block(Nil))
            case _~_~p~_~st~Some(_~se) => IfS(p,st,se) }
        | failure("illegal start of statement")
       )

  def stype: Parser[Type]
      = simpleType ~ rep( "->" ~ stype ) ^^
        { case d~ts => ts.foldRight(d){ case (_~t,r) => FunctionType(r,t) } }

  def simpleType: Parser[Type]
      = ( ident ~ "[" ~ rep1sep( stype, "," ) ~ "]" ^^
          { case n~_~ts~_ => ParametricType(n,ts) }
        | "(" ~ rep1sep( stype, "," ) ~ ")" ^^
          { case _~ts~_ => if (ts.length==1) ts.head else TupleType(ts) }
        | "<" ~ rep1sep( ident ~ ":" ~ stype, "," ) ~ ">" ^^
          { case _~cs~_ => RecordType(cs.map{ case n~_~t => (n,t)}.toMap) }
        | ident ^^ { s => BasicType(s) }
        )

  def program: Parser[Stmt]
      = rep( stmt ~ ";" ) ^^
        { ss => Block(ss.map{ case s~_ => s }) }

  /** Parse a statement */
  def parse ( line: String ): Stmt
      = phrase(program)(new lexical.Scanner(line)) match {
          case Success(e,_) => e:Stmt
          case m => println(m); Block(Nil)
        }

  def main ( args: Array[String] ) {
    println(edu.uta.diql.core.Pretty.print(parse(args(0)).toString))
  }
}
