package edu.uta.diql

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox.Context
import scala.language.experimental.macros


object SparkCodeGenerator extends DistributedCodeGenerator {
  import edu.uta.diql.{CodeGeneration => cg}
  import cg._

  /** Default Spark implementation of the algebraic operations
   *  used for type-checking in CodeGenerator.code
   */
  def cMap[A,B] ( f: (A) => TraversableOnce[B], S: RDD[A] ) (implicit tag: ClassTag[B]): RDD[B]
    = S.flatMap[B](f)

  // bogus; used for type-checking only
  def cMap2[A,B] ( f: (A) => RDD[B], S: RDD[A] ) (implicit tag: ClassTag[B]): RDD[B]
    = S.flatMap[B](f(_).collect)

  // bogus; used for type-checking only
  def cMap[A,B] ( f: (A) => RDD[B], S: Iterable[A] ): RDD[B]
    = f(S.head)

  def groupBy[K,A] ( S: PairRDDFunctions[K,A] ) (implicit kt: ClassTag[K]): RDD[(K,Iterable[A])]
    = S.groupByKey()

  def orderBy[K,A] ( S: RDD[(K,A)] ) ( implicit ord: Ordering[K], kt: ClassTag[K], at: ClassTag[A] ): RDD[A]
    = S.sortBy(_._1).values

  def reduce[A] ( acc: (A,A) => A, S: RDD[A] ): A
    = S.reduce(acc)

  def coGroup[K,A,B] ( X: RDD[(K,A)], Y: RDD[(K,B)] )
          (implicit kt: ClassTag[K], vt: ClassTag[A]): RDD[(K,(Iterable[A],Iterable[B]))]
    = X.cogroup(Y)

  def cross[A,B] ( X: RDD[A], Y: RDD[B] ) (implicit bt: ClassTag[B]): RDD[(A,B)]
    = X.cartesian(Y)

  def merge[A] ( X: RDD[A], Y: RDD[A] ): RDD[A]
    = X++Y

  def broadcastCogroupLeft[K,A,B] ( X: RDD[(K,A)], Y: PairRDDFunctions[K,B] ): RDD[(K,(Iterable[A],Iterable[B]))] = {
    val bc = X.sparkContext.broadcast(X.collect().groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    Y.groupByKey().flatMap( y => bc.value.get(y._1) match {
                                    case Some(xs) => List((y._1,(xs,y._2)))
                                    case _ => Nil
                                 } )
  }

  def broadcastCogroupRight[K,A,B] ( X: PairRDDFunctions[K,A], Y: RDD[(K,B)] ): RDD[(K,(Iterable[A],Iterable[B]))] = {
    val bc = Y.sparkContext.broadcast(Y.collect().groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    X.groupByKey().flatMap( x => bc.value.get(x._1) match {
                                    case Some(ys) => List((x._1,(x._2,ys)))
                                    case _ => Nil
                                 } )
  }

  def broadcastJoinLeft[K,A,B] ( X: RDD[(K,A)], Y: RDD[(K,B)] ): RDD[(K,(A,B))] = {
    val bc = X.sparkContext.broadcast(X.collect().groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    Y.flatMap( y => bc.value.get(y._1) match {
                            case Some(xs) => xs.map( x => (y._1,(x,y._2)) )
                            case _ => Nil
                      } )
  }

  def broadcastJoinRight[K,A,B] ( X: RDD[(K,A)], Y: RDD[(K,B)] ): RDD[(K,(A,B))] = {
    val bc = Y.sparkContext.broadcast(Y.collect().groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    X.flatMap( x => bc.value.get(x._1) match {
                            case Some(ys) => ys.map( y => (x._1,(x._2,y)) )
                            case _ => Nil
                      } )
  }

  def broadcastCrossLeft[A,B] ( X: RDD[A], Y: RDD[B] ) (implicit at: ClassTag[A]): RDD[(A,B)] = {
    val bc = X.sparkContext.broadcast(X.collect())
    Y.flatMap( y => bc.value.map( x => (x,y) ) )
  }

  def broadcastCrossRight[A,B] ( X: RDD[A], Y: RDD[B] ) (implicit bt: ClassTag[B]): RDD[(A,B)] = {
    val bc = Y.sparkContext.broadcast(Y.collect())
    X.flatMap( x => bc.value.map( y => (x,y) ) )
  }

  /** The Spark code generator for algebraic terms */
  def codeGen ( c: Context ) ( e: Expr, env: Map[c.Tree,c.Tree] ): c.Tree = {
    import c.universe._
    e match {
      case MatchE(x,List(Case(VarPat(v),BoolConst(true),b)))
        if AST.occurences(v,b) > 1 && distr(x)
        => val xc = codeGen(c)(x,env)
           val tp = getType(c)(xc,env)
           val vc = TermName(v)
           val bc = codeGen(c)(b,env+((q"$vc",tp)))
           return q"{ val $vc = $xc.cache(); $bc }"
      case _ =>
    if (!distr(e))   // if e is not an RDD operation, use the code generation for Traversable
       return cg.codeGen(c)(e,env,codeGen(c)(_,_))
    else e match {
      case cMap(Lambda(TuplePat(List(VarPat(v),_)),Elem(Var(_v))),
                groupBy(x))
        if _v == v
        => val xc = codeGen(c)(x,env)
           q"$xc.distinct()"
      case cMap(Lambda(TuplePat(List(k,vs)),
                       Elem(Tuple(List(k_,reduce(m,vs_))))),
                groupBy(x))
        if k_ == k && vs_ == vs
        => val xc = codeGen(c)(x,env)
           val fm = TermName(method_name(m))
           monoid(c,m) match {
             case Some(mc) => q"$xc.foldByKey($mc)(_ $fm _)"
             case _ => q"$xc.reduceByKey(_ $fm _)"
           }
      case cMap(Lambda(p@TuplePat(List(k,TuplePat(List(xs,ys)))),
                       cMap(Lambda(px,cMap(Lambda(py,Elem(b)),ys_)),xs_)),
                coGroup(x,y))
        if xs_ == AST.toExpr(xs) && ys_ == AST.toExpr(ys)
           && AST.occurences(AST.patvars(xs)++AST.patvars(ys),b) == 0
           && irrefutable(p)
        => val xc = codeGen(c)(x,env)
           val yc = codeGen(c)(y,env)
           val kc = code(k,c)
           val pxc = code(px,c)
           val pyc = code(py,c)
           val bc = codeGen(c)(b,env)
           val join = if (smallDataset(x))
                         q"distr.broadcastJoinLeft($xc,$yc)"
                      else if (smallDataset(y))
                         q"distr.broadcastJoinRight($xc,$yc)"
                      else q"$xc.join($yc)"
           q"$join.map{ case ($kc,($pxc,$pyc)) => $bc }"
      case cMap(Lambda(p,Elem(b)),x)
        if irrefutable(p)
        => val pc = code(p,c)
           val (_,tp,xc) = cg.typedCode(c)(x,env,codeGen(c)(_,_))
           val bc = codeGen(c)(b,env+((pc,tp)))
           q"$xc.map{ case $pc => $bc }"
      case cMap(Lambda(p,IfE(d,Elem(b),Empty())),x)
        if irrefutable(p)
        => val pc = code(p,c)
           val (_,tp,xc) = cg.typedCode(c)(x,env,codeGen(c)(_,_))
           val dc = codeGen(c)(d,env+((pc,tp)))
           val bc = codeGen(c)(b,env+((pc,tp)))
           q"$xc.filter{ case $pc => $dc }.map{ case $pc => $bc }"
      case cMap(Lambda(p,b),x)
        => val pc = code(p,c)
           val (_,tp,xc) = cg.typedCode(c)(x,env,codeGen(c)(_,_))
           val bc = codeGen(c)(b,env+((pc,tp)))
           if (irrefutable(p))
              q"$xc.flatMap{ case $pc => $bc }"
           else q"$xc.flatMap{ case $pc => $bc; case _ => Nil }"
      case groupBy(x)
        => val xc = codeGen(c)(x,env)
           q"$xc.groupByKey()"
      case orderBy(x)
        => val xc = codeGen(c)(x,env)
           // q"$xc.sortBy(_._1).values doesn't work correctly in local mode
           q"$xc.sortBy(_._1,true,1).values"
      case coGroup(x,y)
        => val xc = codeGen(c)(x,env)
           val yc = codeGen(c)(y,env)
           if (smallDataset(x))
              q"distr.broadcastCogroupLeft($xc,$yc)"
           else if (smallDataset(y))
              q"distr.broadcastCogroupRight($xc,$yc)"
           else q"$xc.cogroup($yc)"
      case cross(x,y)
        => val xc = codeGen(c)(x,env)
           val yc = codeGen(c)(y,env)
           if (smallDataset(x))
              q"distr.broadcastCrossLeft($xc,$yc)"
           else if (smallDataset(y))
              q"distr.broadcastCrossRight($xc,$yc)"
           else q"$xc.cartesian($yc)"
      case reduce(m,x)
        => val (_,tp,xc) = typedCode(c)(x,env,codeGen(c)(_,_))
           val fm = accumulator(c)(m,tp)
           monoid(c,m) match {
             case Some(mc) => q"$xc.fold($mc)($fm)"
             case _ => q"$xc.reduce($fm)"
           }
      case Merge(x,y)
        => val xc = codeGen(c)(x,env)
           val yc = codeGen(c)(y,env)
           q"$xc++$yc"
      case _ => cg.codeGen(c)(e,env,codeGen(c)(_,_))
    } }
  }
}
