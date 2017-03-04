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
import scala.reflect.macros.TypecheckException
import scala.language.experimental.macros
import java.io._

object CodeGeneration {

  val char_maps = Map( '+' -> "plus", '-' -> "minus", '*' -> "times", '/' -> "div", '%' -> "percent",
                       '|' -> "bar", '&' -> "bar", '!' -> "bang", '^' -> "up", '~' -> "tilde",
                       '=' -> "eq", '<' -> "less", '>' -> "greater", ':' -> "colon", '?' -> "qmark",
                       '\\' -> "bslash" )

  /** Scala translates special chars in method names to $names */
  def method_name ( n: String ): String =
    n.foldLeft(""){ case (r,c) => r+(char_maps.get(c) match {
                                        case Some(s) => '$'+s; case _ => c }) }

  /** return the reduce accumulation function of the monoid with name n;
   *  can be either infix or binary method
   */
  def accumulator ( c: Context ) ( n: String, tp: c.Tree ): c.Tree = {
    import c.universe._
    val f = TermName(method_name(n))
    val acc = q"(x:$tp,y:$tp) => x.$f(y)"
    getOptionalType(c)(acc,Map()) match {
      case Left(_) => acc
      case _ => val acc = q"(x:$tp,y:$tp) => $f(x,y)"
                getOptionalType(c)(acc,Map()) match {
                  case Left(_) => acc
                  case Right(ex) => throw ex
                }
    }
  }

  /** Translate a Pattern to a Scala pattern */
  def code ( p: Pattern, c: Context ): c.Tree = {
    import c.universe._
    p match {
      case TuplePat(ps)
        => val psc = ps.map(code(_,c))
           pq"(..$psc)"
      case NamedPat(n,p)
        => val pc = code(p,c)
           val nc = TermName(n)
           pq"$nc@$pc"
      case CallPat(n,ps:+RestPat(v))
        => val psc = ps.map(code(_,c))
           val f = TermName(method_name(n))
           val tv = TermName(v)
           if (v=="_") pq"$f(..$psc,_*)"
              else pq"$f(..$psc,$tv@_*)"
      case CallPat(n,ps)
        => val psc = ps.map(code(_,c))
           val f = TermName(method_name(n))
           pq"$f(..$psc)"
      case MethodCallPat(p,m,ps:+RestPat(v))
        => val pc = code(p,c)
           val psc = ps.map(code(_,c))
           val f = TermName(method_name(m))
           val tv = TermName(v)
           if (v=="_") pq"$pc.$f(..$psc,_*)"
              else pq"$pc.$f(..$psc,$tv@_*)"
      case MethodCallPat(p,m,ps)
        => val pc = code(p,c)
           val psc = ps.map(code(_,c))
           val f = TermName(method_name(m))
           pq"$pc.$f(..$psc)"
      case StringPat(s)
        => pq"$s"
      case CharPat(s)
        => pq"$s"
      case LongPat(n)
        => pq"$n"
      case IntPat(n)
        => pq"$n"
      case DoublePat(n)
        => pq"$n"
      case BooleanPat(n)
        => pq"$n"
      case VarPat(v)
        => val tv = TermName(v)
           pq"$tv"
      case _ => pq"_"
    }
  }

  /** Return the range type of functionals */
  def returned_type ( c: Context ) ( tp: c.Tree ): c.Tree = {
    import c.universe._
    tp match {
       case tq"$d => $r"
         => returned_type(c)(r)
       case _ => tp
    }
  }

  /** Return the type of Scala code, if exists
   *  @param code Scala code
   *  @param env an environment that maps patterns to types
   *  @return the type of code, if the code is typechecked without errors
   */
  def getOptionalType ( c: Context ) ( code: c.Tree, env: Map[c.Tree,c.Tree] ): Either[c.Tree,TypecheckException] = {
    import c.universe._
    val fc = env.foldLeft(code){ case (r,(p,tq"Any"))
                                   => q"{ case $p => $r }"
                                 case (r,(p,tp))
                                   => val nv = TermName(c.freshName("x"))
                                      q"($nv:$tp) => $nv match { case $p => $r }" }
    val te = try c.Expr[Any](c.typecheck(q"{ import edu.uta.diql._; $fc }")).actualType
             catch { case ex: TypecheckException => return Right(ex) }
    val Typed(_,ftp) = c.parse("x:("+te+")")
    Left(returned_type(c)(ftp))
  }

  /** Return the type of Scala code
   *  @param code Scala code
   *  @param env an environment that maps patterns to types
   *  @return the type of code
   */
  def getType ( c: Context ) ( code: c.Tree, env: Map[c.Tree,c.Tree] ): c.Tree = {
    import c.universe._
    getOptionalType(c)(code,env) match {
      case Left(tp) => tp
      case Right(ex)
        => if (debug) {
              println("Code: "+code)
              println("Bindings: "+env)
              val sw = new StringWriter
              ex.printStackTrace(new PrintWriter(sw))
              println(sw.toString)
            }
            throw new Error("*** Typechecking error during macro expansion: "+ex.msg)
    }
  }

  /** Return type information about the expression e and store it in e.tpe */
  def typedCode ( c: Context ) ( e: Expr, env: Map[c.Tree,c.Tree],
                  cont: (Expr,Map[c.Tree,c.Tree]) => c.Tree ): (String,c.Tree,c.Tree) = {
    import c.universe._
    val ec = cont(e,env)
    if (e.tpe != null)
       e.tpe match {
         case tp: (String,c.Tree,c.Tree) @unchecked
           => return (tp._1,tp._2,ec)
       }
    val tp = getType(c)(ec,env)
    val atp = c.Expr[Any](c.typecheck(tp,c.TYPEmode)).actualType
    val evaluator = if (atp <:< edu.uta.diql.distr.typeof(c))   // subset of a distributed dataset
                       "distr"
                    else if (atp <:< typeOf[Traversable[_]] || atp <:< typeOf[Array[_]])
                       "algebra"
                    else throw new Error("*** Type "+tp+" of expression "+ec+" is not a collection type")
    val ctp = c.Expr[Any](c.typecheck(if (evaluator=="distr")
                                         q"(x:$tp) => x.first()"
                                      else q"(x:$tp) => x.head")).actualType
    val Typed(_,etp) = c.parse("x:("+ctp+")")
    val ret = (evaluator,returned_type(c)(etp),ec)
    e.tpe = ret
    ret
  }

  /** Is this pattern irrefutable (always matches)? */
  def irrefutable ( p: Pattern ): Boolean =
    p match {
    case CallPat(_,_) | MethodCallPat(_,_,_) | StringPat(_) | IntPat(_)
       | LongPat(_) | DoublePat(_) | BooleanPat(_) => false
    case _ => AST.accumulatePat[Boolean](p,irrefutable(_),_&&_,true)
  }

  /** Eta expansion for method and constructor argument list to remove the placeholder syntax
   *  e.g., _+_ is expanded to (x,y) => x+y
   */
  def codeList ( c: Context ) ( es: List[Expr], f: List[c.Tree] => c.Tree,
                                env: Map[c.Tree,c.Tree],
                                cont: (Expr,Map[c.Tree,c.Tree]) => c.Tree ): c.Tree = {
    import c.universe._
    val n = es.map{ case Var("_") => 1; case _ => 0 }.fold(0)(_+_)
    if (n == 0)
       return f(es.map(cont(_,env)))
    val ns = es.map{ case Var("_") => { val nv = TermName(c.freshName("x"))
                                        (nv,q"$nv":c.Tree) }
                     case e => (null,cont(e,env)) }
    val tpt = tq""  // empty type
    val vs = ns.flatMap{ case (null,_) => Nil; case (v,_) => List(q"val $v: $tpt") }
    val ne = f(ns.map(_._2))
    q"(..$vs) => $ne"
  }

  /** Generic Scala code generation that works for both Traversable and distributed collections.
   *  It does not generate optimized code. It is used for type inference using Scala's
   *  typecheck and for embedding type info into the code.
   */
  def code ( c: Context ) ( e: Expr, env: Map[c.Tree,c.Tree],
                            cont: (Expr,Map[c.Tree,c.Tree]) => c.Tree ): c.Tree = {
    import c.universe._
    e match {
      case flatMap(Lambda(p,b),x)
        => val pc = code(p,c)
           val nv = TermName(c.freshName("x"))
           val (px,tp,xc) = typedCode(c)(x,env,cont)
           val (pb,_,bc) = typedCode(c)(b,env+((pc,tp)),cont)
           val pcx = TermName(if (px=="distr" || pb=="distr") "distr" else "algebra") 
           val cmapf = TermName(if (px=="distr" && pb=="distr") "flatMap2" else "flatMap")
           if (irrefutable(p))
              q"$pcx.$cmapf(($nv:$tp) => $nv match { case $pc => $bc },$xc)"
           else q"$pcx.$cmapf(($nv:$tp) => $nv match { case $pc => $bc; case _ => Nil },$xc)"
      case groupBy(x)
        => val (pck,tp,xc) = typedCode(c)(x,env,cont)
           q"${pck:TermName}.groupBy($xc)"
      case orderBy(x)
        => val (pck,tp,xc) = typedCode(c)(x,env,cont)
           q"${pck:TermName}.orderBy($xc)"
      case coGroup(x,y)
        => val (px,_,xc) = typedCode(c)(x,env,cont)
           val (py,_,yc) = typedCode(c)(y,env,cont)
           if (px != py)
              println("*** Cannot join a distributed with a local dataset: "+e)
           q"${px:TermName}.coGroup($xc,$yc)"
      case cross(x,y)
        => val (px,_,xc) = typedCode(c)(x,env,cont)
           val (py,_,yc) = typedCode(c)(y,env,cont)
           if (px != py)
              println("*** Cannot join a distributed with a local dataset: "+e)
           q"${px:TermName}.cross($xc,$yc)"
      case reduce(m,x)
        => val (pck,tp,xc) = typedCode(c)(x,env,cont)
           val fm = accumulator(c)(m,tp)
           monoid(c,m) match {
             case Some(mc) => q"$xc.fold($mc:$tp)($fm)"
             case _ => q"${pck:TermName}.reduce[$tp]($fm,$xc)"
           }
      case SmallDataSet(x)
        => val (pck,tp,xc) = typedCode(c)(x,env,cont)
           xc
      case Tuple(es)
        => codeList(c)(es,cs => q"(..$cs)",env,cont)
      case Call(n,es)
        => val fm = TermName(method_name(n))
           codeList(c)(es,cs => q"$fm(..$cs)",env,cont)
      case Constructor(n,es)
        => val fm = TypeName(n)
           codeList(c)(es,cs => q"new $fm(..$cs)",env,cont)
      case MethodCall(Var("_"),m,null)
        => val nv = TermName(c.freshName("x"))
           val fm = TermName(method_name(m))
           val tpt = tq""  // empty type
           val p = q"val $nv: $tpt"
           q"($p) => $nv.$fm"
      case MethodCall(x,m,null)
        => val xc = cont(x,env)
           val fm = TermName(method_name(m))
           q"$xc.$fm"
      case MethodCall(x,"=",List(y))
        => val xc = cont(x,env)
           val yc = cont(y,env)
           q"$xc = $yc"
      case MethodCall(x,m,es)
        => val fm = TermName(method_name(m))
           codeList(c)(x+:es,{ case cx+:cs => q"$cx.$fm(..$cs)" },env,cont)
      case Elem(x)
        => val xc = cont(x,env)
           q"List($xc)"
      case Empty()
        => q"List()"
      case Merge(x,y)
        => val (px,_,xc) = typedCode(c)(x,env,cont)
           val (py,_,yc) = typedCode(c)(y,env,cont)
           if (px != py)
              println("*** Cannot merge distributed with local datasets: "+e+" "+px+" "+py)
           q"${px:TermName}.merge($xc,$yc)"
      case IfE(p,x,y)
        => val pc = cont(p,env)
           val xc = cont(x,env)
           val yc = cont(y,env)
           q"if ($pc) $xc else $yc"
      case MatchE(x,cs)
        => val xc = cont(x,env)
           val tp = getType(c)(xc,env)
           val cases = cs.map{ case Case(p,BoolConst(true),b)
                                 => val pc = code(p,c)
                                    val bc = cont(b,env+((pc,tp)))
                                    cq"$pc => $bc"
                               case Case(p,n,b)
                                 => val pc = code(p,c)
                                    val nc = cont(n,env)
                                    val bc = cont(b,env+((pc,tp)))
                                    cq"$pc if $nc => $bc"
                             }
           q"$xc match { case ..$cases }"
      case Lambda(p,b)
        => val pc = code(p,c)
           val bc = cont(b,env+((pc,tq"Any")))
           q"{ case $pc => $bc }"
      case Nth(x,n)
        => val xc = cont(x,env)
           val nc = TermName("_"+n)
           q"$xc.$nc"
      case IntConst(n)
        => q"$n"
      case LongConst(n)
        => q"$n"
      case DoubleConst(n)
        => q"$n"
      case StringConst(s)
        => q"$s"
      case CharConst(s)
        => q"$s"
      case BoolConst(n)
        => q"$n"
      case Var(v)
        => Ident(TermName(v))
      case _ => throw new Exception("Unrecognized AST: "+e)
    }
  }

  /** Generate Scala code for Traversable (in-memory) collections */
  def codeGen ( c: Context ) ( e: Expr, env: Map[c.Tree,c.Tree],
                               cont: (Expr,Map[c.Tree,c.Tree]) => c.Tree ): c.Tree = {
    import c.universe._
    e match {
      case flatMap(Lambda(p,Elem(b)),x)
        if irrefutable(p)
        => val pc = code(p,c)
           val (_,tp,xc) = typedCode(c)(x,env,cont)
           val nv = TermName(c.freshName("x"))
           val bc = cont(b,env+((pc,tp)))
           q"$xc.map(($nv:$tp) => $nv match { case $pc => $bc })"
      case flatMap(Lambda(p,b),x)
        => val pc = code(p,c)
           val (_,tp,xc) = typedCode(c)(x,env,cont)
           val nv = TermName(c.freshName("x"))
           val bc = cont(b,env+((pc,tp)))
           if (irrefutable(p))
              q"$xc.flatMap(($nv:$tp) => $nv match { case $pc => $bc })"
           else q"$xc.flatMap(($nv:$tp) => $nv match { case $pc => $bc; case _ => Nil })"
      case groupBy(x)
        => val xc = cont(x,env)
           q"$xc.groupBy(_._1).mapValues( _.map(_._2))"
      case orderBy(x)
        => val xc = cont(x,env)
           q"$xc.sortBy(_._1).map(_._2)"
      case coGroup(x,y)
        => val xc = cont(x,env)
           val yc = cont(y,env)
           q"algebra.coGroup($xc,$yc)"
      case cross(x,y)
        => val xc = cont(x,env)
           val yc = cont(y,env)
           val xv = c.freshName("x")
           val yv = c.freshName("y")
           q"$xc.flatMap($xv => $yc.map($yv => ($xv,$yv)))"
      case reduce(m,x)
        => val (_,tp,xc) = typedCode(c)(x,env,cont)
           val nv = TermName(c.freshName("x"))
           val fm = accumulator(c)(m,tp)
           monoid(c,m) match {
             case Some(mc) => q"$xc.foldLeft[$tp]($mc)($fm)"
             case _ => q"$xc.reduce[$tp]($fm)"
           }
      case Merge(x,y)
        => val xc = cont(x,env)
           val yc = cont(y,env)
           q"$xc++$yc"
      case _ => code(c)(e,env,cont)
    }
  }

  /** Does this expression return a distributed collection (such as, an RDD)? */
  def distr ( e: Expr ): Boolean = {
    e match {
      case coGroup(_,_) => true
      case cross(_,_) => true
      case _ => val t = e match {
              case flatMap(_,x) => x.tpe
              case groupBy(x) => x.tpe
              case reduce(m,x) => x.tpe
              case orderBy(x) => x.tpe
              case SmallDataSet(x) => x.tpe
              case _ => e.tpe
             }
            if (t == null)
               return false
            val (mode,_,_) = t
            mode == "distr"
    }
  }

  def smallDataset ( e: Expr ): Boolean =
    e match {
      case SmallDataSet(_) => true
      case flatMap(_,x) => smallDataset(x)
      case groupBy(x) => smallDataset(x)
      case orderBy(x) => smallDataset(x)
      case coGroup(x,y) => smallDataset(x) && smallDataset(y)
      case cross(x,y) => smallDataset(x) && smallDataset(y)
      case _ => false
  }
 }
