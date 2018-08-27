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

import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.token.StdTokens
import scala.util.matching.Regex
import scala.util.parsing.input.Position


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
      = regex("""[0-9]+[Ll]""".r) ^^ { LongLit(_) }

  /* floating point numbers */
  def doubleLit: Parser[Token]
      = regex("""[0-9]*[\.][0-9]+([eE][+-]?[0-9]+)?[FfDd]?""".r) ^^ { DoubleLit(_) }

  /* character literal */
  def charLit: Parser[Token]
      = regex("""'[^']'""".r) ^^ { CharLit(_) }

  /* an infix operator can be any sequence of special chars, except delimiters, etc */ 
  def infixOpr: Parser[Token]
      = regex("""[^\s\w\$\(\)\[\]\{\}\'\"\`\.\;\,\\/]+""".r) ^^
        { s => if (delimiters.contains(s)) Keyword(s) else InfixOpr(s) }
}

object Parser extends StandardTokenParsers {
  var queryText: String = ""

  override val lexical = new MyLexical

  lexical.delimiters += ( "(" , ")" , "[", "]", "{", "}", "," , ":", ";", ".", "<-", "<--", "=>", "@", "::",
                          "||", "&&", "!", "=", "==", "<=", ">=", "<", ">", "!=", "+", "-", "*", "/", "%",
                          "^", "|", "&" )

  lexical.reserved += ("trace", "group", "order", "by", "having", "select", "distinct", "from", "where", "in",
                       "some", "all", "let", "repeat", "step", "limit", "abstract", "do", "finally", "import", "until",
                       "object", "return", "trait", "var", "case", "else", "for", "lazy", "override",
                       "sealed", "try", "while", "catch", "extends", "forSome", "match", "package",
                       "super", "true", "with", "class", "false", "if", "new", "private", "this",
                       "type", "yield", "def", "final", "implicit", "null", "protected", "throw", "val")

  /* groups of infix operator precedence, from low to high */
  val operator_precedence: List[Parser[String]]
      = List( "||"|"|", "^", "&&"|"&", "<="|">="|"<"|">", "="|"=="|"!=", "+"|"-", "*"|"/"|"%" )

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
        { op => (x:Expr,y:Expr) => MethodCall(x,op,List(y)) }
  def infix ( level: Int ): Parser[Expr]
      = if (level >= precedence.length) conses
        else infix(level+1) * terms(level)

  def fromRaw ( s: String ): String = s.replaceAllLiterally("""\n""","\n")
        
  def expr: Parser[Expr]
      = infix(0) | conses

  def sem: Parser[Option[String]] = opt( ";" )

  def char: Parser[String]
      = accept("char literal",{ case t: lexical.CharLit => t.chars })

  def int: Parser[Int]
      = numericLit ^^ { _.toInt }

  def long: Parser[Long]
      = accept("long literal",{ case t: lexical.LongLit => t.chars.init.toLong })

  def double: Parser[Double]
      = accept("double literal",{ case t: lexical.DoubleLit => t.chars.toDouble })

  def conses: Parser[Expr]      /* :: is treated specially: right associative, flips operands */
      = rep1sep( matches, "::" ) ^^
        { es => es.init.foldRight(es.last)
                  { case (e,r) => MethodCall(r,"::",List(e)) } }
  def matches: Parser[Expr]
      = factor ~ rep( "match" ~ "{" ~ rep1sep( "case" ~ pat ~ opt( "by" ~> expr ) ~ "=>" ~ expr, sem ) ~ "}" ) ^^
        { case a~as
            => as.foldLeft(a){ case (r,_~_~cs~_)
                                 => MatchE(r,cs.map{ case _~p~Some(c)~_~b => Case(p,c,b)
                                                     case _~p~_~_~b => Case(p,BoolConst(true),b) }) } }
  def factor: Parser[Expr]
      = term ~ rep( opt( "." ) ~ ident ~ opt( "(" ~> repsep( expr, "," ) <~ ")"
                                            | expr ^^ {(x:Expr) => List(x)} ) ) ^^
        { case a~as => as.foldLeft(a){ case (r,_~n~Some(xs)) => MethodCall(r,n,xs)
                                       case (r,_~"desc"~_) => Constructor("Inv",List(r))
                                       case (r,_~"asc"~_) => r
                                       case (r,_~n~_) => MethodCall(r,n,null) } }
  def term: Parser[Expr]
      = ( "select" ~ opt( "distinct" ) ~ expr ~ "from" ~ rep1sep( qual, "," ) ~
                     opt( "where" ~> expr ) ~ groupBy ~ orderBy ^^
          { case _~Some(_)~e~_~qs~Some(w)~gb~ob
              => SelectDistQuery(e,qs:+Predicate(w),gb,ob)
            case _~Some(_)~e~_~qs~_~gb~ob
              => SelectDistQuery(e,qs,gb,ob)
            case _~_~e~_~qs~Some(w)~gb~ob
              => SelectQuery(e,qs:+Predicate(w),gb,ob)
            case _~_~e~_~qs~_~gb~ob
              => SelectQuery(e,qs,gb,ob) }
        | "some" ~ rep1sep( qual, "," ) ~ ":" ~ expr ^^
          { case _~qs~_~e => SomeQuery(e,qs) }
        | "all" ~ rep1sep( qual, "," ) ~ ":" ~ expr ^^
          { case _~qs~_~e => AllQuery(e,qs) }
        | "repeat" ~ pat ~ "=" ~ expr ~ "step" ~ expr
                   ~ opt( "until" ~> expr ) ~ opt( "limit" ~> expr ) ^^
          { case _~p~_~e~_~b~Some(w)~Some(n)
              => repeat(Lambda(p,b),e,Lambda(p,w),n)
            case _~p~_~e~_~b~Some(w)~None
              => repeat(Lambda(p,b),e,Lambda(p,w),IntConst(Int.MaxValue))
            case _~p~_~e~_~b~None~Some(n)
              => repeat(Lambda(p,b),e,Lambda(p,BoolConst(false)),n)
            case _ => throw new Exception("A repeat clause must specify an until condition and/or a limit")
          }
        | "let" ~ rep1sep( pat ~ "=" ~ expr, ",") ~ "in" ~ expr ^^
          { case _~ps~_~b => ps.foldRight(b){ case (p~_~e,r) => MatchE(e,List(Case(p,BoolConst(true),r))) } }
        | "if" ~ "(" ~ expr ~ ")" ~ expr ~ "else" ~ expr ^^
          { case _~_~p~_~t~_~e => IfE(p,t,e) }
        | ident ~ "(" ~ repsep( expr, "," ) ~ ")" ^^
          { case n~_~es~_ => Call(n,es) }
        | "new" ~> ident ~ opt( "(" ~> repsep( expr, "," ) <~ ")" ) ^^
          { case n~Some(es) => Constructor(n,es)
            case n~_ => Constructor(n,Nil) }
        | "trace" ~ "(" ~ positioned(expr) ~ positioned(")" ^^ { StringConst(_) })
          ^^ { case _~_~s~rp => Call("trace",List(StringConst(subquery(s.pos,rp.pos)),s)) }
        | "true" ^^^ { BoolConst(true) }
        | "false" ^^^ { BoolConst(false) }
        | ( "-" | "+" | "!" ) ~ expr ^^
          { case o~e => MethodCall(e,"unary_"+o,null) }
        | allInfixOpr ~ "/" ~ term ^^
          { case op~_~e => reduce(BaseMonoid(op),e) }
        | "{" ~> rep1sep( "case" ~ pat ~ opt( "by" ~> expr ) ~ "=>" ~ expr, sem ) <~ "}" ^^
          { cs => { val nv = AST.newvar
                    Lambda(VarPat(nv),
                           MatchE(Var(nv),
                                  cs.map{ case _~p~Some(c)~_~b => Case(p,c,b)
                                          case _~p~_~_~b => Case(p,BoolConst(true),b) })) } }
        | "(" ~ repsep( pat, "," ) ~ ")" ~ "=>" ~ expr ^^
          { case _~ps~_~_~b => Lambda(TuplePat(ps),b) }
        | ident ~ "=>" ~ expr ^^
          { case v~_~b => Lambda(VarPat(v),b) }
        | "(" ~ repsep( expr, "," ) ~ ")" ^^
          { case _~es~_ => if (es.length==1) es.head else Tuple(es) }
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
        | ident ^^
          { s => Var(s) }
        | failure("illegal start of expression")
        )
  def qual: Parser[Qualifier]
      = ( pat ~ ("<-" | "<--" | "=") ~ expr ^^
          { case p~"in"~e => Generator(p,e)
            case p~"<-"~e => Generator(p,e)
            case p~"<--"~e => Generator(p,SmallDataSet(e))
            case p~"="~e => LetBinding(p,e) }
        | failure("illegal start of qualifier")
        )
  def pat: Parser[Pattern]
      = spat ~ rep( ( ident | infixOpr | "::" ) ~ spat ) ^^
        { case p~ps => ps.foldLeft(p){ case (r,op~np) => MethodCallPat(r,op,List(np)) } }
  def spat: Parser[Pattern]
      = ( "(" ~ repsep( pat, "," ) ~ ")"
          ^^ { case _~ps~_ => if (ps.length==1) ps.head else TuplePat(ps) }
        | ident ~ "(" ~ repsep( pat, "," ) ~ opt( "*" ) <~ ")" ^^
          { case n~_~(ps:+NamedPat(a,StarPat()))~Some(_) => CallPat(n,ps:+RestPat(a))
            case n~_~(ps:+StarPat())~Some(_) => CallPat(n,ps:+RestPat("_"))
            case _~_~_~Some(_) => throw new Exception("Wrong star pattern")
            case n~_~ps~_ => CallPat(n,ps) }
        | "true" ^^^ { BooleanPat(true) }
        | "false" ^^^ { BooleanPat(false) }
        | ident ~ "@" ~ pat
          ^^ { case n~_~p => if (n=="_") p else NamedPat(n,p) }
        | "_"
          ^^^ { StarPat() }
        | ident
          ^^ { s => if (s == "_") StarPat() else VarPat(s) }
        | double ^^
          { s => DoublePat(s) }
        | long ^^
          { s => LongPat(s) }
        | int ^^
          { s => IntPat(s) }
        | stringLit ^^
          { s => StringPat(fromRaw(s)) }
        | char ^^
          { s => CharPat(fromRaw(s).apply(1)) }
        | failure("illegal start of pattern")
        )
  def groupBy: Parser[Option[GroupByQual]]
      = opt( "group" ~ "by" ~ pat ~ opt( ":" ~> expr ) ~ opt( cogroup ) ~ opt( "having" ~> expr ) ) ^^
        { case Some(_~_~p~Some(e)~cg~Some(h)) => Some(GroupByQual(p,e,h,cg))
          case Some(_~_~p~Some(e)~cg~_) => Some(GroupByQual(p,e,BoolConst(true),cg))
          case Some(_~_~p~_~cg~Some(h)) => Some(GroupByQual(p,AST.toExpr(p),h,cg))
          case Some(_~_~p~_~cg~_) => Some(GroupByQual(p,AST.toExpr(p),BoolConst(true),cg))
          case _ => None
        }
  def cogroup: Parser[CoGroupQual]
    = "from" ~ rep1sep( qual, "," ) ~ opt( "where" ~> expr ) ~ "group" ~ "by" ~ pat ~ opt( ":" ~> expr ) ^^
      { case _~qs~Some(w)~_~_~p~Some(e) => CoGroupQual(qs:+Predicate(w),p,e)
        case _~qs~Some(w)~_~_~p~_ => CoGroupQual(qs:+Predicate(w),p,AST.toExpr(p))
        case _~qs~_~_~_~p~Some(e) => CoGroupQual(qs,p,e)
        case _~qs~_~_~_~p~_ => CoGroupQual(qs,p,AST.toExpr(p))
      }
  def orderBy: Parser[Option[OrderByQual]]
      = opt( "order" ~ "by" ~ expr ) ^^
        { case Some(_~_~e) => Some(OrderByQual(e))
          case _ => None
        }
  def pexpr: Parser[Expr]
      = positioned(expr)
  def block: Parser[List[Expr]]
      = rep1sep( pexpr, sem )
  def stype: Parser[Type]
      = ( ident ~ "[" ~ rep1sep( stype, "," ) ~ "]" ^^
          { case n~_~ts~_ => ParametricType(n,ts) }
        | "(" ~ rep1sep( stype, "," ) ~ ")" ^^
          { case _~ts~_ => TupleType(ts) }
        | ident ^^ { s => BasicType(s) }
        )
  def macrodef: Parser[(String,List[(String,Type)],Expr)]
      = "def" ~ ( allInfixOpr | ident ) ~ "(" ~ rep1sep( ident ~ ":" ~ stype, "," ) ~ ")" ~
              "=" ~ pexpr ^^
        { case _~n~_~ps~_~_~b => (n,ps.map{ case m~_~t => (m,t) },b) }
  def macrodefs: Parser[List[(String,List[(String,Type)],Expr)]]
      = rep1sep( macrodef, sem )

  def subquery ( from: Position, to: Position ): String = {
    val lines = queryText.split("\n")
    lines(to.line-1) = lines(to.line-1).substring(0,to.column-1)
    val c = lines.slice(from.line-1,to.line)
    c(0) = c(0).substring(from.column-1)
    "( "+from.toString+") "+c.mkString(" ")
  }

  /** Parse a query */
  def parse ( line: String ): Expr = {
      queryText = line
      phrase(pexpr)(new lexical.Scanner(line)) match {
          case Success(e,_) => e:Expr
          case m => println(m); Tuple(Nil)
      }
  }

  /** Parse many queries */
  def parseMany ( line: String ): List[Expr] = {
      queryText = line
      phrase(block)(new lexical.Scanner(line)) match {
          case Success(e,_) => e:List[Expr]
          case m => println(m); Nil
      }
  }

  /** Parse a macro definition */
  def parseMacros ( line: String ): List[(String,List[(String,Type)],Expr)]
      = phrase(macrodefs)(new lexical.Scanner(line)) match {
          case Success(e,_) => e
          case m => println(m); null
      }

  def main ( args: Array[String] ) {
    println("input : "+ args(0))
    println(Pretty.print(parse(args(0)).toString))
  }
}
