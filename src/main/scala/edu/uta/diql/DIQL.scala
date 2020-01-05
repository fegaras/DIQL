/*
 * Copyright © 2017, 2018 University of Texas at Arlington
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
package edu.uta

import scala.reflect.macros.whitebox.Context
import scala.language.experimental.macros
import scala.util.parsing.input.Position
import scala.language.implicitConversions


package object diql {
  import core._
  import Parser.{parse,parseMany,parseMacros}

  private var tab_count = -3
  private var trace_count = 0L

  /** evaluate a DIQL expression and print tracing info */
  def trace[T] ( msg: String, value: => T ): T = {
    tab_count += 3
    trace_count += 1
    val c = trace_count
    println(" "*tab_count+"*** "+c+": "+msg)
    val v = value
    print(" "*tab_count+"--> "+c+": ")
    println(v)
    tab_count -= 3
    v
  }

  var showErased = false

  def provenance[T] ( value: T, loc: Int, lineage: Iterable[Lineage] ): LiftedResult[T]
    = ResultValue(value,UnaryLineage(loc,value,lineage))

  def provenance[T] ( value: T, loc: Int, left: Iterable[Lineage], right: Iterable[Lineage] ): LiftedResult[T]
    = ResultValue(value,BinaryLineage(loc,value,left,right))

  def liftFlatMap[S,T] ( f: S => Iterable[T], result: LiftedResult[S], loc: Int ): Iterable[LiftedResult[T]]
    = result match {
        case ResultValue(v,q)
          => try { val s = f(v)
                   if (showErased && s.isEmpty)
                      List(ErasedValue[T](UnaryLineage(loc,v,List(q))))
                   else s.map(v => ResultValue[T](v,UnaryLineage(loc,v,List(q))))
             } catch { case ex: Throwable
                         => List(ErrorValue[T]("flatMap: "+ex.toString,q)) }
        case ErasedValue(q)
          => if (showErased) List(ErasedValue[T](q)) else Nil
        case ErrorValue(m,q)
          => List(ErrorValue[T](m,q))
  }

  def propagateLineage[S,T] ( x: LiftedResult[S] ): Iterable[LiftedResult[T]]
    = x match {
        case ErasedValue(q) if showErased => List(ErasedValue[T](q))
        case ErrorValue(m,q) => List(ErrorValue[T](m,q))
        case _ => Nil
      }

  def debugInMemory[T] ( value: LiftedResult[T], exprs: List[String] ): T = {
    val debugger = new Debugger(Array(value),exprs)
    debugger.debug()
    value match {
      case ResultValue(v,_) => v
      case ErrorValue(m,_) => throw new Error(m)
      case _ => throw new Error("error")
    }
  }

  def debugInMemory[T] ( value: Iterable[LiftedResult[T]], exprs: List[String] ): Iterable[T] = {
    val debugger = new Debugger(value.toArray,exprs)
    debugger.debug()
    value.flatMap{ case ResultValue(v,_) => List(v) case _ => Nil }
  }

  def translate_query ( query: String ): Expr = {
    if (diql_explain)
       println("\nQuery:\n"+query)
    translate_query(parse(query))
  }

  def translate_query ( query: Expr): Expr = {
    val e = Normalizer.normalizeAll(Translator.translate(query))
    if (diql_explain)
       println("Algebraic term:\n"+Pretty.print(e.toString))
    e
  }

  def q_impl ( c: Context ) ( query: c.Expr[String] ): c.Expr[Any] = {
    import c.universe._
    val Literal(Constant(s:String)) = query.tree
    val cg = new { val context: c.type = c } with QueryCodeGenerator
    cg.code_generator(translate_query(s),s,query.tree.pos.line,false)
  }

  /** translate the query to Scala code */
  def q ( query: String ): Any = macro q_impl

  def debug_impl ( c: Context ) ( query: c.Expr[String] ): c.Expr[Any] = {
    import c.universe._
    showErased = false
    val Literal(Constant(s:String)) = query.tree
    val cg = new { val context: c.type = c } with QueryCodeGenerator
    cg.code_generator(translate_query(s),s,query.tree.pos.line,true)
  }

  def debugAll_impl ( c: Context ) ( query: c.Expr[String] ): c.Expr[Any] = {
    import c.universe._
    showErased = true
    val Literal(Constant(s:String)) = query.tree
    val cg = new { val context: c.type = c } with QueryCodeGenerator
    cg.code_generator(translate_query(s),s,query.tree.pos.line,true)
  }

  /** translate the query to Scala code that debugs the query */
  def debug ( query: String ): Any = macro debug_impl
  def debugAll ( query: String ): Any = macro debugAll_impl

  private def subquery ( lines: List[String], from: Position, to: Position ): String = {
    val c = (if (to == null) lines.drop(from.line-1) else lines.slice(from.line-1,to.line)).toArray
    c(0) = c(0).substring(from.column-1)
    if (to != null)
       c(c.length-1) = c(c.length-1).substring(0,to.column-1)
    c.mkString("\n").split(';')(0)
  }

  def qs_impl ( c: Context ) ( query: c.Expr[String] ): c.Expr[List[Any]] = {
    import c.universe._
    val cg = new { val context: c.type = c } with QueryCodeGenerator
    val Literal(Constant(s:String)) = query.tree
    val lines = s.split("\n").toList
    val el = parseMany(s)
    val ec = ( for { i <- Range(0,el.length-1)
                   } yield cg.code_generator(translate_query(el(i)),
                               (i+1)+") "+subquery(lines,el(i).pos,el(i+1).pos),
                               query.tree.pos.line+el(i).pos.line-1,false)
             ).toList :+ cg.code_generator(translate_query(el.last),
                               el.length+") "+subquery(lines,el.last.pos,null),
                               query.tree.pos.line+el.last.pos.line-1,false)
    c.Expr[List[Any]](q"List(..$ec)")
  }

  /** compile many DIQL queries to Scala code that returns List[Any] */
  def qs ( query: String ): List[Any] = macro qs_impl

  def explain_impl ( c: Context ) ( b: c.Expr[Boolean] ): c.Expr[Unit] = {
    import c.universe._
    b.tree match {
      case Literal(Constant(bv:Boolean))
        => diql_explain = bv
      case _ => ;
    }
    c.Expr[Unit](q"()")
  }

  /** turn on/off compilation tracing mode */
  def explain ( b: Boolean ): Unit = macro explain_impl

  def m_impl ( c: Context ) ( macroDef: c.Expr[String] ): c.Expr[Unit] = {
    import c.universe._
    val qcg = new { val context: c.type = c } with QueryCodeGenerator
    val Literal(Constant(s:String)) = macroDef.tree
    val lines = s.split("\n").toList
    val el = parseMacros(s)
    def macro_def ( i: Int )
      = el(i) match {
          case (nm,vars,e)
            => val env = vars.map{ case (n,tp) => (qcg.cg.code(VarPat(n)),qcg.cg.Type2Tree(tp)) }.toMap
               val npos = if (i+1>=el.length) null else el(i+1)._3.pos
               qcg.code_generator(translate_query(e), (i+1)+") "+subquery(lines,e.pos,npos),
                                  macroDef.tree.pos.line+e.pos.line-1,
                                  false,env)
               macro_defs += ((nm,(vars,e)))
        }
    for { i <- el.indices } macro_def(i)
    c.Expr[Unit](q"()")
  }

  /** macro declaration */
  def m ( macroDef: String ): Unit = macro m_impl

  def monoid_impl ( c: Context ) ( monoid: c.Expr[String], zero: c.Expr[Any] ): c.Expr[Unit] = {
    import c.universe._
    monoid.tree match {
      case Literal(Constant(m:String))
        => if (monoids.contains(m))
              monoids = monoids-m
           if (showCode(zero.tree) == "null")
              monoids = monoids+((m,null))
           else monoids = monoids+((m,showCode(zero.tree)))
      case _ => ;
    }
    c.Expr[Unit](q"()")
  }

  /** specify a new monoid */
  def monoid ( monoid: String, zero: Any ): Unit = macro monoid_impl

  def stream_impl ( c: Context ) ( query: c.Expr[String] ): c.Expr[Any] = {
    import c.universe._
    val Literal(Constant(s:String)) = query.tree
    val cg = new { val context: c.type = c } with QueryCodeGenerator
    diql_streaming = true
    val ci = cg.code_generator(translate_query(s),s,query.tree.pos.line,false)
    diql_streaming = false
    ci
  }

  /** translate the query to Scala code */
  def stream ( query: String ): Any = macro stream_impl

  def range ( n1: Long, n2: Long, n3: Long ): Seq[Long]
    = n1.to(n2,n3).toSeq

  def inRange ( i: Long, n1: Long, n2: Long, n3: Long ): Boolean
    = i>=n1 && i<= n2 && (i-n1)%n3 == 0

  def v_impl ( c: Context ) ( sc: c.Expr[Any] = null, query: c.Expr[String] ): c.Expr[Any] = {
    import c.universe._
    import edu.uta.diablo._
    val Literal(Constant(s:String)) = query.tree
    val scontext = if (sc == null) q"null" else sc.tree
    ComprehensionTranslator.datasetClassPath = core.distributed.datasetClassPath
    ComprehensionTranslator.datasetClass = core.distributed.datasetClassPath.split('.').last
    val cg = new { val context: c.type = c } with QueryCodeGenerator
    // hooks to the Scala compiler
    Typechecker.typecheck_call
      = ( f: String, args: List[diablo.Type] )
          => { val ts = args.map(ComprehensionTranslator.translate)
               cg.cg.typecheck_call(f,ts).map(ComprehensionTranslator.translate)
             }
    Typechecker.typecheck_method
      = ( o: diablo.Type, m: String, args: List[diablo.Type] )
          => { val ts = if (args == null) null
                        else args.map(ComprehensionTranslator.translate)
               val to = ComprehensionTranslator.translate(o)
               cg.cg.typecheck_method(to,m,ts).map(ComprehensionTranslator.translate)
             }
    Typechecker.typecheck_var
      = ( v: String ) => cg.cg.typecheck_var(v).map(ComprehensionTranslator.translate)
    val (qc,bs) = Diablo.translate_query(s)
    val env = bs.mapValues(x => cg.cg.Type2Tree(x))
    val ds = env.map {
               case (nm,tq"Int")
                 => val v = TermName(nm)
                    q"var $v: Int = 0"
               case (nm,tq"Long")
                 => val v = TermName(nm)
                    q"var $v: Long = 0L"
               case (nm,tq"Double")
                 => val v = TermName(nm)
                    q"var $v: Double = 0D"
               case (nm,tq"Boolean")
                 => val v = TermName(nm)
                    q"var $v: Boolean = false"
               case (nm,tp)
                 => val v = TermName(nm)
                    q"var $v: $tp = null"
             } ++
             Translator.external_variables
                  .mapValues(x => cg.cg.Type2Tree(ComprehensionTranslator.translate(x)))
                  .map{ case (nm,tp)
                          => val v = TermName(nm)
                             q"$v:$tp"
                      }
    val tenv = env.map{ case (v,tp)
                          => val tv = TermName(v)
                             (pq"$tv":Tree,tp)
                      }
    def isInMemory ( tp: core.Type ): Boolean
      = tp match {
          case core.ParametricType(f,List(_))
            => f == "Traversable" || f == "Array" || f == "Seq"
          case _ => false
        }
    def vtype ( v: String ): Tree = {
        val tv = TermName(v)
        Typechecker.typecheck(Var(v),Translator.global_variables) match {
            case ParametricType(_,tps)
              => cg.cg.Type2Tree(ComprehensionTranslator.translate(tps.last))
            case _ => cg.cg.getType(q"$tv.first()._2",tenv)
        }
    }
    def code ( e: Code[core.Expr] ): Tree
      = e match {
          case Assignment(v,core.Call("increment",List(_,core.StringConst(op),x)))
            => if (diql_explain)
                  println("Translating increment:\n"+v+" = "+Pretty.print(x.toString))
               val c = cg.code_generator(x,s,query.tree.pos.line,false,tenv)
               val tv = TermName(v)
               val tp = vtype(v)
               val fm = cg.cg.accumulator(core.BaseMonoid(op),tp,x)
               val cc = q"core.distributed.merge($tv,$fm,$c)"
               q"$tv = $cc"
          case Assignment(v,core.Call("update",List(_,x)))
            => if (diql_explain)
                  println("Translating update:\n"+v+" = "+Pretty.print(x.toString))
               val c = cg.code_generator(x,s,query.tree.pos.line,false,tenv)
               val tv = TermName(v)
               val tp = vtype(v)
               val cc = q"core.distributed.merge($tv,(x:$tp,y:$tp) => y,$c)"
               q"$tv = $cc"
          case Assignment(v,core.Call("Some",List(x)))
            => if (diql_explain)
                  println("Translating assignment:\n"+v+" = "+Pretty.print(x.toString))
               val c = cg.code_generator(x,s,query.tree.pos.line,false,tenv)
               val tv = TermName(v)
               Typechecker.typecheck(Var(v),Translator.global_variables) match {
                   case ParametricType(_,_)
                     => val vtp = cg.cg.typecheck2type(core.Var(v),tenv)
                        val xtp = cg.cg.typecheck2type(x,tenv)
                        (vtp,xtp) match {
                          case (Some(vtpe),Some(xtpe))
                            if isInMemory(vtpe) && isInMemory(xtpe)
                            => q"$tv = $c.toArray"
                          case (Some(vtpe),_)
                            if isInMemory(vtpe)
                            => q"$tv = core.distributed.collect($c)"
                          case (_,Some(xtpe))
                            if isInMemory(xtpe)
                            => q"$tv = core.distributed.initialize($scontext,$c)"
                          case _
                            => q"$tv = core.distributed.cache($c)"
                        }
                   case _ => q"$tv = $c"
               }
          case Assignment(v,x)
            => if (diql_explain)
                  println("Translating assignment:\n"+v+" = "+Pretty.print(x.toString))
               val c = cg.code_generator(x,s,query.tree.pos.line,false,tenv)
               val tv = TermName(v)
               q"$tv = $c.head"
          case CodeC(core.Call("Some",List(core.Call("Some",List(v)))))
            => val c = cg.code_generator(v,s,query.tree.pos.line,false,tenv)
               q"$c"
          case CodeC(v)
            => val c = cg.code_generator(v,s,query.tree.pos.line,false,tenv)
               q"$c"
          case WhileLoop(core.Call("Some",List(p)),b)
            => val pc = cg.code_generator(p,s,query.tree.pos.line,false,tenv)
               val bc = code(b)
               q"while($pc) $bc"
          case WhileLoop(p,b)
            => val pc = cg.code_generator(p,s,query.tree.pos.line,false,tenv)
               val bc = code(b)
               q"while($pc.get) $bc"
          case CodeBlock(cbs)
            => val cbsc = cbs.map(x => code(x))
               q"{ ..$cbsc }"
        }
    val qcc = code(qc)
    if (diql_explain)
       println("Scala Code:\n"+ q"{ ..$ds; ..$qcc }")
    c.Expr[Any](q"{ ..$ds; ..$qcc }")
  }

  def v_impl2 ( c: Context ) ( query: c.Expr[String] ): c.Expr[Any]
    = v_impl(c)(null,query)

  /** translate imperative loop-based programs to Scala code */
  def v ( sc: Any, query: String ): Any = macro v_impl

  /** translate imperative loop-based programs to Scala code */
  def v ( query: String ): Any = macro v_impl2
}
