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

import scala.reflect.macros.whitebox.Context
import scala.reflect.macros.TypecheckException
import scala.language.experimental.macros
import java.io._


abstract class CodeGeneration {
  val c: Context
  import c.universe.{Expr=>_,_}
  import AST._

  var line: Int = 0

  /** contains bindings from patterns to Scala types */
  type Environment = Map[c.Tree,c.Tree]

  /** add a new binding from a pattern to a Scala type in the Environment */
  def add ( p: Pattern, tp: c.Tree, env: Environment ): Environment = {
    p.tpe = tp
    env + ((code(p),tp))
  }

  val char_maps = Map( '+' -> "plus", '-' -> "minus", '*' -> "times", '/' -> "div", '%' -> "percent",
                       '|' -> "bar", '&' -> "amp", '!' -> "bang", '^' -> "up", '~' -> "tilde",
                       '=' -> "eq", '<' -> "less", '>' -> "greater", ':' -> "colon", '?' -> "qmark",
                       '\\' -> "bslash" )

  /** Scala translates special chars in method names to $names */
  def method_name ( n: String ): String =
    n.foldLeft(""){ case (r,d) => r+(char_maps.get(d) match { case Some(s) => '$'+s; case _ => d }) }

  /** return the reduce accumulation function of the monoid with name n;
   *  can be either infix or binary method
   */
  def accumulator ( monoid: Monoid, tp: c.Tree, e: Expr ): c.Tree =
    monoid match {
      case BaseMonoid(n)
        => val f = TermName(method_name(n))
           val acc = q"(x:$tp,y:$tp) => x.$f(y)"
           getOptionalType(acc,Map()) match {
              case Left(_) => acc
              case _
                => val acc = q"(x:$tp,y:$tp) => $f(x,y)"
                   getOptionalType(acc,Map()) match {
                      case Left(_) => acc
                      case Right(ex)
                        => println(s"Wrong accumulator of type ($tp,$tp)->$tp\nin $e")
                           throw ex
                   }
           }
      case ParametricMonoid(_,m)
        => accumulator(m,tp,e)
      case _ => throw new Error("Unexpected monoid: "+monoid)
    }

  /** Return the range type of functionals */
  def returned_type ( tp: c.Tree ): c.Tree = {
    tp match {
       case tq"$d => $r"
         => returned_type(r)
       case _ => tp
    }
  }
  
  /** convert a Type to a Tree. There must be a better way to do this */
  def type2tree ( tp: c.Type ): c.Tree = {
    val ntp = if (tp <:< c.typeOf[AnyVal]) tp.toString.split('(')(0) else tp
    val Typed(_,etp) = c.parse("x:("+ntp+")")
    etp
  }

  def Tree2Type ( tp: c.Tree ): edu.uta.diql.core.Type =
    tp match {
      case tq"(..$cs)" if cs.length > 1
        => TupleType(cs.map(Tree2Type(_)))
      case tq"$n[..$cs]" if cs.nonEmpty
        => ParametricType(n.toString,cs.map(Tree2Type(_)))
      case _
        => BasicType(tp.toString)
    }

  def Type2Tree ( tp: edu.uta.diql.core.Type ): c.Tree =
    tp match {
      case TupleType(ts)
        => val cs = ts.map(Type2Tree(_))
           tq"(..$cs)"
      case ParametricType(n,ts)
        => val cs = ts.map(Type2Tree(_))
           val nc = TypeName(n)
           tq"$nc[..$cs]"
      case BasicType(btp)
        => val nc = TypeName(btp)
           tq"$nc"
    }

  def ExprElemType ( x: Expr ): edu.uta.diql.core.Type = {
    val (_,tpc:c.Tree,_) = x.tpe
    Tree2Type(tpc)
  }

  /** Return the type of Scala code, if exists
   *  @param code Scala code
   *  @param env an environment that maps patterns to types
   *  @return the type of code, if the code is typechecked without errors
   */
  def getOptionalType ( code: c.Tree, env: Environment ): Either[c.Tree,TypecheckException] = {
    val fc = env.foldLeft(code){ case (r,(p,tq"Any"))
                                   => q"{ case $p => $r }"
                                 case (r,(p,tp))
                                   => val nv = TermName(c.freshName("x"))
                                      q"($nv:$tp) => $nv match { case $p => $r }" }
    val te = try c.Expr[Any](c.typecheck(q"{ import edu.uta.diql._; $fc }")).actualType
             catch { case ex: TypecheckException => return Right(ex) }
    Left(returned_type(type2tree(te)))
  }

  /** Return the type of Scala code
   *  @param code Scala code
   *  @param env an environment that maps patterns to types
   *  @return the type of code
   */
  def getType ( code: c.Tree, env: Environment ): c.Tree = {
    getOptionalType(code,env) match {
      case Left(tp) => tp
      case Right(ex)
        => if (diql_explain) {
              println(s"Typechecking error at line $line: ${ex.msg}")
              println("Code: "+code)
              println("Bindings: "+env)
              val sw = new StringWriter
              ex.printStackTrace(new PrintWriter(sw))
              println(sw.toString)
            }
            c.abort(c.universe.NoPosition,s"Typechecking error at line $line: ${ex.msg}")
    }
  }

  /** Typecheck the query using the Scala's typechecker */
  def typecheck ( query: Expr, env: Environment = Map() ): c.Tree = {
    def rec ( e: Expr, env: Environment ): c.Tree
        = code(e,env,rec(_,_))
    getType(code(query,env,rec(_,_)),env)
  }

  /** is x equal to the path to the distributed package? */
  def isDistr ( x: c.Tree ): Boolean =
    x.equalsStructure(q"core.distributed")

  /** Return type information about the expression e and store it in e.tpe */
  def typedCodeOpt ( ec: c.Tree, tp: c.Tree, env: Environment,
                     cont: (Expr,Environment) => c.Tree ): Option[(c.Tree,c.Tree,c.Tree)] = {
    val atp = c.Expr[Any](c.typecheck(tp,c.TYPEmode)).actualType
    val evaluator = if (atp <:< distributed.typeof(c))   // subset of a distributed dataset
                       q"core.distributed"
                    else if (atp <:< typeOf[Traversable[_]] || atp <:< typeOf[Array[_]])
                       q"core.inMemory"
                    else return None
    val ctp = c.Expr[Any](c.typecheck(if (isDistr(evaluator))
                                         q"(x:$tp) => $evaluator.head(x)"
                                      else q"(x:$tp) => x.head")).actualType
    Some((evaluator,returned_type(type2tree(ctp)),ec))
  }

  /** Return type information about the expression e and store it in e.tpe */
  def typedCode ( e: Expr, env: Environment,
                  cont: (Expr,Environment) => c.Tree ): (c.Tree,c.Tree,c.Tree) = {
    val ec = cont(e,env)
    val tp = getType(ec,env)
    if (e.tpe != null)
       e.tpe match {
         case tp: (c.Tree,c.Tree,c.Tree) @unchecked
           => return (tp._1,tp._2,ec)
       }
    typedCodeOpt(ec,tp,env,cont) match {
      case Some(v) => e.tpe = v; v
      case _ => c.abort(c.universe.NoPosition,
                        s"Type $tp of expression $ec is not a collection type (line $line)")
    }
  }

  /** Translate a Pattern to a Scala pattern */
  def code ( p: Pattern ): c.Tree = {
    import c.universe._
    p match {
      case TuplePat(ps)
        => val psc = ps.map(code(_))
           pq"(..$psc)"
      case NamedPat(n,np)
        => val pc = code(np)
           val nc = TermName(n)
           pq"$nc@$pc"
      case CallPat(n,ps:+RestPat(v))
        => val psc = ps.map(code(_))
           val f = TermName(method_name(n))
           val tv = TermName(v)
           if (v=="_") pq"$f(..$psc,_*)"
              else pq"$f(..$psc,$tv@_*)"
      case CallPat(n,ps)
        => val psc = ps.map(code(_))
           val f = TermName(method_name(n))
           pq"$f(..$psc)"
      case MethodCallPat(np,m,ps:+RestPat(v))
        => val pc = code(np)
           val psc = ps.map(code(_))
           val f = TermName(method_name(m))
           val tv = TermName(v)
           if (v=="_") pq"$f($pc,..$psc,_*)"
              else pq"$f($pc,..$psc,$tv@_*)"
      case MethodCallPat(np,m,ps)
        => val pc = code(np)
           val psc = ps.map(code(_))
           val f = TermName(method_name(m))
           pq"$f($pc,..$psc)"
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

  def repeatCoercedType ( tp: c.Tree ): c.Tree = {
    tp match {
      case tq"(..$ts)" if ts.length > 1
        => val s = ts.map(repeatCoercedType(_))
           tq"(..$s)"
      case _
        => val atp = c.Expr[Any](c.typecheck(tp,c.TYPEmode)).actualType
           if (atp <:< distributed.typeof(c))
              distributed.mkType(c)(returned_type(type2tree(c.Expr[Any](c.typecheck(q"(x:$atp) => core.distributed.head(x)")).actualType)))
           else if (atp <:< typeOf[Traversable[_]] || atp <:< typeOf[Array[_]]) {
              val etp = returned_type(type2tree(c.Expr[Any](c.typecheck(q"(x:$atp) => x.head")).actualType))
              tq"Array[$etp]"
           } else tp
    }
  }

  def repeatInitCoercion ( tp: c.Tree, e: c.Tree ): c.Tree = {
    tp match {
      case tq"(..$ts)" if ts.length > 1
        => val s = (ts zip (1 to ts.length)).map{ case (t,i)
                        => val n = TermName("_"+i)
                           repeatInitCoercion(t,q"$e.$n") }
           q"(..$s)"
      case _
        => val atp = c.Expr[Any](c.typecheck(tp,c.TYPEmode)).actualType
           if (atp <:< distributed.typeof(c))
              e
           else if (atp <:< typeOf[Traversable[_]] || atp <:< typeOf[Array[_]])
              q"$e.toArray"
           else e
    }
  }

  def repeatStepCoercion ( itp: c.Tree, stp: c.Tree, e: c.Tree ): c.Tree = {
    (itp,stp) match {
      case (tq"(..$its)",tq"(..$sts)") if its.length > 1
        => val s = (its zip sts zip (1 to its.length)).map{ case ((it,st),i)
                        => val n = TermName("_"+i)
                           repeatStepCoercion(it,st,q"$e.$n") }
           q"(..$s)"
      case _
        => val it = c.Expr[Any](c.typecheck(itp,c.TYPEmode)).actualType
           val st = c.Expr[Any](c.typecheck(stp,c.TYPEmode)).actualType
           if (it <:< distributed.typeof(c))
              q"core.distributed.cache($e)"
           else if (it <:< typeOf[Traversable[_]] || it <:< typeOf[Array[_]])
             if (st <:< typeOf[Traversable[_]] || st <:< typeOf[Array[_]])
                q"$e.toArray"
             else q"core.distributed.collect($e).toArray"
           else e
    }
  }

  /** Is this pattern irrefutable (always matches)? */
  def irrefutable ( p: Pattern ): Boolean =
    p match {
      case CallPat(_,_) | MethodCallPat(_,_,_) | StringPat(_) | IntPat(_)
         | LongPat(_) | DoublePat(_) | BooleanPat(_) => false
      case _ => accumulatePat[Boolean](p,irrefutable(_),_&&_,true)
    }

  /** Eta expansion for method and constructor argument list to remove the placeholder syntax
   *  e.g., _+_ is expanded to (x,y) => x+y
   */
  def codeList ( es: List[Expr], f: List[c.Tree] => c.Tree, env: Environment,
                  cont: (Expr,Environment) => c.Tree ): c.Tree = {
    val n = es.map{ case Var("_") => 1; case _ => 0 }.sum
    if (n == 0)
       return f(es.map(cont(_,env)))
    val ns = es.map{ case Var("_") => val nv = TermName(c.freshName("x"))
                                      (nv,q"$nv":c.Tree)
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
  def code ( e: Expr, env: Environment,
             cont: (Expr,Environment) => c.Tree ): c.Tree = {
    e match {
      case flatMap(Lambda(p,b),x)
        => val pc = code(p)
           val nv = TermName(c.freshName("x"))
           val (px,tp,xc) = typedCode(x,env,cont)
           val (pb,_,bc) = typedCode(b,add(p,tp,env),cont)
           val pcx = if (isDistr(px) || isDistr(pb))
                        q"core.distributed"
                     else q"core.inMemory" 
           val cmapf = TermName(if (isDistr(px) && isDistr(pb))
                                   "flatMap2"
                                else "flatMap")
           if (irrefutable(p) || isDistr(px) || isDistr(pb))
              q"$pcx.$cmapf(($nv:$tp) => $nv match { case $pc => $bc },$xc)"
           else q"$pcx.$cmapf(($nv:$tp) => $nv match { case $pc => $bc; case _ => Nil },$xc)"
      case groupBy(x)
        => val (pck,_,xc) = typedCode(x,env,cont)
           q"$pck.groupBy($xc)"
      case orderBy(x)
        => val (pck,_,xc) = typedCode(x,env,cont)
           q"$pck.orderBy($xc)"
      case coGroup(x,y)
        => val (px,_,xc) = typedCode(x,env,cont)
           val (py,_,yc) = typedCode(y,env,cont)
           if (isDistr(px))
              q"$px.coGroup($xc,$yc)"
           else q"$py.coGroup($xc,$yc)"
      case cross(x,y)
        => val (px,_,xc) = typedCode(x,env,cont)
           val (py,_,yc) = typedCode(y,env,cont)
           if (isDistr(px))
              q"$px.cross($xc,$yc)"
           else q"$py.cross($xc,$yc)"
      case reduce(m,x)
        => val (pck,tp,xc) = typedCode(x,env,cont)
           val fm = accumulator(m,tp,e)
           q"$pck.reduce[$tp]($fm,$xc)"
      case repeat(Lambda(p,step),init,Lambda(_,cond),n)
        => val nv = TermName(c.freshName("v"))
           val iv = TermName(c.freshName("i"))
           val bv = TermName(c.freshName("b"))
           val ret = TermName(c.freshName("ret"))
           val xv = TermName(c.freshName("x"))
           val nc = cont(n,env)
           val pc = code(p)
           val ic = cont(init,env)
           val itp = getType(ic,env)
           val otp = repeatCoercedType(itp)
           val nenv = add(p,otp,env)
           val sc = cont(step,nenv)
           val stp = getType(sc,nenv)
           val cc = cont(cond,nenv)
           val iret = repeatInitCoercion(itp,q"$xv")
           val sret = repeatStepCoercion(itp,stp,q"$xv")
           q"""{ var $bv = true
                 var $iv = 0
                 var $ret: $otp = $ic match { case $xv => $iret }
                 do { $ret match {
                         case $nv@$pc
                           => $ret = $sc match { case $xv => $sret }
                              $iv = $iv+1
                              $bv = $cc
                         } 
                    } while(!$bv && $iv < $nc)
                 $ret
               }"""
      case SmallDataSet(x)
        => typedCode(x,env,cont)
           cont(x,env)
      case Call("avg_value",List(x))
        => val xc = cont(x,env)
           q"$xc.value"
      case Tuple(es)
        => codeList(es,cs => q"(..$cs)",env,cont)
      case Call("debug",List(x,BoolConst(true),es))
        => cont(x,env)
           val esc = cont(es,env)
           val (_,tq"$f[$tp]",xc) = typedCode(x,env,cont)
           q"core.distributed.debug[$tp]($xc.map(_.asInstanceOf[LiftedResult[$tp]]),$esc)"
      case Call("debug",List(x,_,es))
        => val xc = cont(x,env)
           val esc = cont(es,env)
           q"debugInMemory($xc,$esc)"
      case Call(n,es)
        => val fm = TermName(method_name(n))
           codeList(es,cs => q"$fm(..$cs)",env,cont)
      case Constructor(n,es)
        => val fm = TypeName(n)
           codeList(es,cs => q"new $fm(..$cs)",env,cont)
      case MethodCall(Var("_"),m,null)
        => val nv = TermName(c.freshName("x"))
           val fm = TermName(method_name(m))
           val tpt = tq""  // empty type
           val p = q"val $nv: $tpt"
           q"($p) => $nv.$fm"
      case MethodCall(x,m,null)
        => val xc = cont(x,env)
           val fm = TermName(method_name(m))
           getOptionalType(q"$xc.$fm",env) match {
             case Left(_) => q"$xc.$fm"
             case _ => q"$xc.map(_.$fm)"   // syntactic sugar: x.A for a collection x
           }
      case MethodCall(x,"=",List(y))
        => val xc = cont(x,env)
           val yc = cont(y,env)
           q"$xc = $yc"
      case MethodCall(x,"++",List(y))
        => val (px,_,xc) = typedCode(x,env,cont)
           val (py,_,yc) = typedCode(y,env,cont)
           if (!px.equalsStructure(py))
              c.abort(c.universe.NoPosition,
                      s"Cannot merge distributed with local datasets: $e (line $line)")
           q"$px.merge($xc,$yc)"
      case MethodCall(x,m,es)
      => val fm = TermName(method_name(m))
        codeList(x+:es,{ case cx+:cs => q"$cx.$fm(..$cs)" },env,cont)
      case Elem(x)
        => val xc = cont(x,env)
           q"List($xc)"
      case Empty()
        => q"Nil"
      case Merge(x,y)
        => val (px,_,xc) = typedCode(x,env,cont)
           val (py,_,yc) = typedCode(y,env,cont)
           if (!px.equalsStructure(py))
              c.abort(c.universe.NoPosition,
                      s"Cannot merge distributed with local datasets: $e (line $line)")
           q"$px.merge($xc,$yc)"
      case IfE(p,x,y)
        => val pc = cont(p,env)
           val xc = cont(x,env)
           val yc = cont(y,env)
           q"if ($pc) $xc else $yc"
      case MatchE(x,List(Case(VarPat(v),BoolConst(true),b)))
        if occurrences(v,b) == 1
        => cont(subst(v,x,b),env)
      case MatchE(x,List(Case(p@VarPat(v),BoolConst(true),b)))
        => val xc = cont(x,env)
           val pc = TermName(v)
           val tp = getType(xc,env)
           typedCodeOpt(xc,tp,env,cont) match {
                case Some(t)
                  => val nv = Var(v)
                     nv.tpe = t
                     x.tpe = t
                     val bc = cont(subst(Var(v),nv,b),add(p,tp,env))
                     return q"{ val $pc:$tp = $xc; $bc }"
                case None =>
           } 
           val bc = cont(b,add(p,tp,env))
           q"{ val $pc:$tp = $xc; $bc }"
      case MatchE(x,List(Case(p,BoolConst(true),b)))
        if irrefutable(p)
        => val xc = cont(x,env)
           val tp = getType(xc,env)
           val pc = code(p)
           val bc = cont(b,add(p,tp,env))
           q"{ val $pc:$tp = $xc; $bc }"
      case MatchE(x,cs)
        => val xc = cont(x,env)
           val tp = getType(xc,env)
           val cases = cs.map{ case Case(p,BoolConst(true),b)
                                 => val pc = code(p)
                                    val bc = cont(b,add(p,tp,env))
                                    cq"$pc => $bc"
                               case Case(p,n,b)
                                 => val pc = code(p)
                                    val nc = cont(n,env)
                                    val bc = cont(b,add(p,tp,env))
                                    cq"$pc if $nc => $bc"
                             }
           q"($xc:$tp) match { case ..$cases }"
      case Lambda(p@VarPat(v),b)
        => val tpt = tq""  // empty type
           val vc = TermName(v)
           val bc = cont(b,add(p,tpt,env))
           val pp = q"val $vc: $tpt"
           q"($pp) => $bc"
      case Lambda(p@TuplePat(ps),b)
        if ps.map{ case VarPat(_) => true; case _ => false }.reduce(_&&_)
        => val tpt = tq""  // empty type
           val vs = ps.map{ case VarPat(v) => TermName(v); case _ => null }
           val pc = vs.map( v => q"val $v: $tpt" )
           val bc = cont(b,add(p,tpt,env))
           q"(..$pc) => $bc"
      case Lambda(p,b)
        => val tpt = tq""  // empty type
           val pc = code(p)
           val bc = cont(b,add(p,tpt,env))
           q"{ case $pc => $bc }"
      case TypedLambda(args,b)
        => val pc = args.map{ case (v,t) => val vc = TermName(v); val tc = Type2Tree(t); q"val $vc:$tc" }
           val vs = args.map( x => VarPat(x._1))
           val ts = args.map(x => Type2Tree(x._2))
           val tpt = tq"(..$ts)"
           val bc = cont(b,add(TuplePat(vs),tpt,env))
           q"(..$pc) => $bc"
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
  def codeGen ( e: Expr, env: Environment,
                cont: (Expr,Environment) => c.Tree ): c.Tree = {
    e match {
      case flatMap(Lambda(p,Elem(b)),x)
        if irrefutable(p)
        => val pc = code(p)
           val (_,tp,xc) = typedCode(x,env,cont)
           val nv = TermName(c.freshName("x"))
           val bc = cont(b,add(p,tp,env))
           q"$xc.map(($nv:$tp) => $nv match { case $pc => $bc })"
      case flatMap(Lambda(p,b),x)
        => val pc = code(p)
           val (_,tp,xc) = typedCode(x,env,cont)
           val nv = TermName(c.freshName("x"))
           val bc = cont(b,add(p,tp,env))
           if (irrefutable(p))
              q"$xc.flatMap(($nv:$tp) => $nv match { case $pc => $bc })"
           else q"$xc.flatMap(($nv:$tp) => $nv match { case $pc => $bc; case _ => Nil })"
      case groupBy(x)
        => val xc = cont(x,env)
           q"$xc.groupBy(_._1).mapValues( _.map(_._2))"
      case MethodCall(orderBy(x),"head",_)
        => val xc = cont(x,env)
           q"$xc.minBy(_._1)._2"
      case MethodCall(orderBy(x),"last",_)
        => val xc = cont(x,env)
           q"$xc.maxBy(_._1)._2"
      case orderBy(x)
        => val xc = cont(x,env)
           q"$xc.sortBy(_._1).map(_._2)"
      case coGroup(x,y)
        => val xc = cont(x,env)
           val yc = cont(y,env)
           q"inMemory.coGroup($xc,$yc)"
      case cross(x,y)
        => val xc = cont(x,env)
           val yc = cont(y,env)
           val xv = c.freshName("x")
           val yv = c.freshName("y")
           q"$xc.flatMap($xv => $yc.map($yv => ($xv,$yv)))"
      case reduce(m,x)
        => val (_,tp,xc) = typedCode(x,env,cont)
           val fm = accumulator(m,tp,e)
           monoid(c,m) match {
             case Some(mc) => q"$xc.foldLeft[$tp]($mc)($fm)"
             case _ => q"$xc.reduce[$tp]($fm)"
           }
      case Call("broadcastVar",List(v))
        => cont(v,env)
      case Merge(x,y)
        => val xc = cont(x,env)
           val yc = cont(y,env)
           q"$xc++$yc"
      case _ => code(e,env,cont)
    }
  }

  /** Does this expression return a distributed collection (such as, an RDD)? */
  def isDistributed ( e: Expr ): Boolean = {
    def tpe ( x: Expr )
      = if (e.tpe == null) isDistributed(x)
        else { val (mode,_,_) = e.tpe
               mode.toString == "core.distributed"
             }
    if (e.tpe != null) {
       val (mode,_,_) = e.tpe
       mode.toString == "core.distributed"
    } else e match {
      case coGroup(_,_) => true
      case cross(_,_) => true
      case repeat(_,x,_,_) => isDistributed(x)
      case SmallDataSet(x) => isDistributed(x)
      case MatchE(_,Case(_,_,a)::_) => isDistributed(a)
      case flatMap(_,x) => tpe(x)
      case groupBy(x) => tpe(x)
      case reduce(_,x) => tpe(x)
      case orderBy(x) => tpe(x)
      case Merge(x,_) => tpe(x)
      case MethodCall(x,"++",List(_)) => tpe(x)
      case Var(_) if e.tpe != null
        => val (mode,_,_) = e.tpe
           mode.toString == "core.distributed"
      case _ => false
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
      case Merge(x,y) => smallDataset(x) && smallDataset(y)
      case _ => !isDistributed(e)
  }
 }
