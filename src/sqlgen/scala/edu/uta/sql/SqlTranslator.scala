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

import edu.uta.diablo._
import scala.collection.mutable.HashMap

object SqlTranslator {
  var datasetClassPath = ""
  val arrayClassName = "Array"
  var datasetClass = ""
  var hm: HashMap[String,String] = HashMap.empty[String,String]

  def bind(p: Pattern, X: Expr): Map[String, Expr] = {
    p match {
      case VarPat(v) =>
        Map(v -> X)
      case TuplePat(ps) =>
        ps.zipWithIndex.map {
          case (pattern, index) =>
          bind(pattern, Nth(X, index + 1)) }.reduce(_ ++ _)

    case _ => throw new Error("Unrecongnized pattern: " + p)
    }
  }

  def compile(m: String, qs: List[Qualifier], bnd: Map[String, Expr]): Map[String, Expr] = {
    qs match {
      case Nil
      => Map[String, Expr]()

      case Generator(p, e) +: ns
      => m match {
        case "gen" =>
          var count = 1
          e match {
            case Var(v) =>
              if (hm.contains(v)) {
              count+=1
              hm += (v -> count.toString)
                var b = bnd.++(bind(p, Var(v.concat(hm(v)))))
                b.++(compile(m, ns, b))
            }
              else {
                hm += (v -> count.toString)
                var b = bnd.++(bind(p, e))
                b.++(compile(m, ns, b))
              }
            case _ => compile(m, ns, bnd)
          }
      }

      case GroupByQual(p, e) +: ns
      => compile(m, ns, bnd)

      case LetBinding(p, e) +: ns
      => compile(m, ns, bnd)

      case Predicate(e) +: ns
      => compile(m, ns, bnd)

      case q :: _ => throw new Error("Unrecognized qualifier: " + q)

    }
  }

  def subst(e: Expr, m: Map[String, Expr]): Expr = {
    e match {
      case Var(name) => m(name)
      case Tuple(args) => Tuple(args.map(subst(_, m)))
      case _ => throw new Error("Unrecognized var: " + e)
    }
  }

  def translateResult (rs: List[Expr], b: Map[String, Expr]) : sql.Expr =
    rs match {
      case Nil => sql.Empty()
      case Var(n)+:ns =>
        sql.Column(List(translate(subst(Var(n), b)), translateResult(ns, b)))
      case Call(e, List(n, s, MethodCall(Project(v, k), m, List(x))))+:ns =>
        sql.MethodCall(sql.Project(translate(subst(v, b)), k), m, List(translate(x)))
      case Project(v, k )+:ns =>
        sql.Column(List(sql.Project(translate(subst(v, b)), k), translateResult(ns, b)))
      case MethodCall(Var(x), o, List(Var(y)))+:ns =>
        sql.Column(List(sql.MethodCall(translate(subst(Var(x),b)), o, List(translate(subst(Var(y),b)))), translateResult(ns, b)))
      case MethodCall(Var(x), o, List(IntConst(v)))+:ns =>
        sql.Column(List(sql.MethodCall(translate(subst(Var(x),b)), o, List(sql.IntConst(v))), translateResult(ns, b)))
      case List(MethodCall(DoubleConst(0.0),_,List(MethodCall(o, m, List(a))))) =>
        a match {
          case Var(n) => sql.MethodCall(translate(subst(o,b)), m, List(sql.Var("@"+n)))
          case _ =>   sql.MethodCall(translate(subst(o,b)), m, List(translate(a)))

        }
      case Tuple(List(v, w))+:ns =>
        sql.Column(List(sql.Tuple(List(translate(subst(v,b)),translate(subst(w,b)))), translateResult(ns, b)))


      // case _ => sql.Empty()
    }

  def translateQualifiers ( m: String, qs: List[Qualifier] , bnd: Map[String, Expr]) : sql.Expr =
    qs match {
      case Nil
      => sql.Empty()

      case Generator(p,e)+:ns
      => m match{
        case "table" =>
          sql.From(List(translate(e)))
        case _ => translateQualifiers(m, ns, bnd)
      }

      case Predicate(e)+:ns
      => m match {
        case "from"
        => e match{
          case MethodCall(o, m, List(x))
          => o match{
            case n if o==x =>
              translateQualifiers("from", ns, bnd)
            case Project(v, k) =>
              x match{
                case Project(w, l) =>
                  sql.From(List(sql.Join(k,l,
                    sql.MethodCall(sql.Project(translate(subst(v, bnd)), k), m, List(sql.Project(translate(subst(w, bnd)), l)))
                  ), translateQualifiers("from", ns, bnd)))
              }
            case Var(_) =>
              x match {
                case IntConst(v) => translateQualifiers(m, ns, bnd)
                case DoubleConst(v) => translateQualifiers(m, ns, bnd)
                case _ =>
                  val t1 = translate(subst(o, bnd))
                  val t2 = translate(subst(x, bnd))
                  val table1 = t1 match {
                    case sql.Nth(sql.Var(t1), _) => t1
                    case sql.Nth(sql.Nth(sql.Var(t1), _), _) => t1
                  }
                  val table2 = t2 match {
                    case sql.Nth(sql.Var(t2), _) => t2
                    case sql.Nth(sql.Nth(sql.Var(t2), _), _) => t2
                  }

                  sql.From(List(sql.Join(table1, table2,
                    sql.MethodCall(translate(subst(o, bnd)), m, List(translate(subst(x, bnd))))),
                    translateQualifiers("from", ns, bnd)))
              }
          }
          case _ => translateQualifiers(m, ns, bnd)

        }
            case "where"
            => e match{
              case Call(inRange, es) =>
                val mid = translate(subst(es.head,bnd))
                val left = translate(es.tail.head)
                var right = translate(es.tail.tail.head)

                right = right match {
                  case sql.MethodCall(sql.Var(n),o,l) => sql.MethodCall(sql.Var("@"+n), o,l)
                  case sql.IntConst(i) => sql.IntConst(i)
                  case sql.Var(n) => sql.Var("@"+n)
                }

                val lExpr = sql.Cond(left,"<=",mid)
                val rExpr = sql.Cond(mid,"<=",right)
                sql.Where(List(sql.Conj( List(lExpr),"AND", List(rExpr)), translateQualifiers("where", ns, bnd) ))
              case MethodCall(o, m, List(x)) =>
                o match {
                  case n if o == x =>
                    translateQualifiers("where", ns, bnd)
                  case Project(v, k) => translateQualifiers("where", ns, bnd)
                  case Var(_) =>
                    x match {
                      case IntConst(_) =>
                        val left = translate(subst(o, bnd))
                        val right = translate(x)
                        sql.Where(List(sql.Cond(left, m, right), translateQualifiers("where", ns, bnd)))

                      case DoubleConst(_) =>
                        val left = translate(subst(o, bnd))
                        val right = translate(x)
                        sql.Where(List(sql.Cond(left, m, right), translateQualifiers("where", ns, bnd)))
                      case _ => sql.Empty()
                    }
                  case _ => sql.Empty()
                }
              case Var(n) =>
                var lExpr = translate(subst(Var(n), bnd))
                var rExpr = sql.BoolConst(true)
                sql.Where(List(sql.Conj( List(lExpr),"=", List(rExpr)), translateQualifiers("where", ns, bnd) ))

            }

        case _ => translateQualifiers(m, ns, bnd)
      }

      case GroupByQual(p,e)+:ns
      => m match {
        case "groupby"
        => e match {
          case Project(v, k) =>
            sql.Project(translate(subst(v, bnd)), k)
          case Tuple(List(x, y)) =>
            sql.Tuple(List(translate(subst(x, bnd)), translate(subst(y, bnd))))
          case Var(x) => translate(subst(Var(x), bnd))
        }
        case _ => translateQualifiers(m, ns, bnd)
      }

      case LetBinding(p,e)+:ns
      => m match {
        case "agg"
        => e match {
            case Project(v, k) =>
              sql.Call ("SUM", List(sql.Project(translate(subst(v, bnd)), k)))
            case MethodCall(Project(v, k), m, List(Project(w, l))) =>
              sql.Call ("SUM", List(sql.MethodCall(sql.Project(translate(subst(v, bnd)), k), m, List(sql.Project(translate(subst(w, bnd)), l)))))
            case MethodCall(MethodCall(Project(v,k),m,List(w)),m2,List(MethodCall(Project(x,l),m3,List(y)))) =>
              sql.Call ("SUM", List(
                sql.MethodCall(sql.MethodCall(sql.Project(translate(subst(v, bnd)), k),m,
                  List(translate(subst(w, bnd)))),m2,
                  List(sql.MethodCall(sql.Project(translate(subst(x, bnd)), l),m3,List(translate(subst(y, bnd))))))))
            case MethodCall(MethodCall(v,m,List(w)),m2,List(MethodCall(v2,m3,List(w2)))) =>
              sql.Call ("SUM", List(
                sql.MethodCall(sql.MethodCall(translate(subst(v, bnd)),m,
                  List(translate(subst(w, bnd)))),m2,
                  List(sql.MethodCall(translate(subst(v2, bnd)),m3,List(translate(subst(w2, bnd))))))))
            case MethodCall(v,o,List(w)) =>
              v match {
                case Var(s) if s.startsWith("v") =>  sql.Call("SUM",List(sql.MethodCall(translate(subst(v, bnd)), o, List(translate(subst(w, bnd))))))
                case Var(c) =>
                  w match {
                    case MethodCall(MethodCall(MethodCall(i,o1,List(v1)),o2,List(v2)),o,List(MethodCall(Var(b),o3,List(v3)))) =>
                      sql.Call("SUM",List(sql.MethodCall(sql.Var("@"+c), o1, List(sql.MethodCall(sql.MethodCall(sql.MethodCall(translate(i),o1,List(translate(subst(v1,bnd)))),o2,List(translate(subst(v2,bnd)))),o,List(sql.MethodCall(sql.Var("@"+b),o3,List(translate(subst(v3, bnd))))))
                  ))))

                    case _ =>  sql.Call("SUM",List(sql.MethodCall(sql.Var("@"+c), o, List(translate(subst(w, bnd))))))

                  }
                case DoubleConst(c) =>
                  w match {
                    case MethodCall(MethodCall(MethodCall(i,o1,List(v1)),o2,List(v2)),o,List(MethodCall(DoubleConst(b),o3,List(v3)))) =>
                      sql.Call("SUM",List(sql.MethodCall(sql.DoubleConst(c), o1, List(sql.MethodCall(sql.MethodCall(sql.MethodCall(translate(i),o1,List(translate(subst(v1,bnd)))),o2,List(translate(subst(v2,bnd)))),o,List(sql.MethodCall(sql.DoubleConst(b),o3,List(translate(subst(v3, bnd))))))
                      ))))
                  }

              }
            case  Call(a,List(v, i)) =>
              sql.Call("SUM", List(sql.Call(a, List(translate(subst(v, bnd)), translate(i)))))
            case Call(a,List(v1, Call(b,List(v2, v3)))) =>
              sql.Call("SUM", List(sql.Call(a, List(translate(subst(v1, bnd)), sql.Call(b, List(translate(subst(v2, bnd)), translate(subst(v3, bnd))))))))
            case IntConst(1) =>
            {
              //println("Hi")
              val w = translateQualifiers("where", ns, bnd)
              //println(qs)
              w match {
                case sql.Where(List(sql.Conj(c,_,List(_)), _)) =>
                  c.head match {
                    case sql.Cond(_, _, right) =>
                      //println("Hi2")
                      sql.Call("COUNT", List(right))
                    case sql.Nth(sql.Var(n),c) => sql.Call("COUNT", List(sql.Nth(sql.Var(n),c)))

                  }
                case _ => sql.Empty()
              }
            }
            case Var(v) =>
              sql.Call ("SUM", List(translate(subst(Var(v), bnd))))
          }
        case _ => translateQualifiers(m, ns, bnd)
      }

      case Predicate(e)+:ns
      =>
        m match {
        case "where"
        => e match{
          case Call(inRange, es) =>
            val mid = translate(subst(es.head,bnd))
            val left = translate(es.tail.head)
            val right = translate(es.tail.tail.head)
            val lExpr = sql.Cond(left,"<=",mid)
            val rExpr = sql.Cond(mid,"<=",right)
            sql.Where(List(sql.Conj( List(lExpr),"and", List(rExpr)), translateQualifiers("where", ns, bnd) ))

          case MethodCall(o, m, List(x))
          => o match{
            case n if o==x => sql.Empty()
            case _ => sql.MethodCall(translate(subst(o, bnd)), m, List(translate(subst(x, bnd))))
          }
        }
        case _ => translateQualifiers(m, ns, bnd)
      }
      case q::_ => throw new Error("Unrecognized qualifier: "+q)
    }

  def translate ( e: Expr ): sql.Expr =
    e match {
      case Var(v)
      => sql.Var(v)
      case Nth(x, n)
      => sql.Nth(translate(x), n)
      case Project(x, a)
      => sql.Project(translate(x), a)
      case Tuple(es)
      => sql.Tuple(es.map(translate))

      case reduce(BaseMonoid("+"), x)
      => translate(x)

      case Call(f, es)
      => es match {
        case List(_, Comprehension(BaseMonoid(bag), Tuple(List(_, IntConst(0))),_)) => sql.Empty()
        case List(_, Comprehension(BaseMonoid(bag), Tuple(List(Tuple(List(_, _)), DoubleConst(0.0))),_)) => sql.Empty()
        case _ => sql.Call(f, es.map(translate))

      }


      case MethodCall(o, m, es)
      => sql.MethodCall(translate(o), m, es.map(translate))

      case Collection(_, cs)
      => sql.Init(cs.map(translate))

      case Comprehension(m, result, qs)
      => var bnd = Map[String, Expr]()
        val b = compile("gen", qs, bnd)
        //println("bnd: "+b)
        hm.clear

        val cols = result match {
          case Tuple(List(v, reduce(x,y))) => sql.Empty()
          case Tuple(args) => translateResult(args, b)
          case _ => sql.Empty()
        }

        val from = sql.From(List(translateQualifiers("from", qs, b)))
        val tables = from match {
          case sql.From(List(sql.Empty())) => translateQualifiers("table", qs, b)
          case _ => from
        }

        sql.Query(
          sql.Select(
            List(sql.Attr(cols
              , Option(List(translateQualifiers("groupby", qs, b)))
              , result match{
                case MethodCall(MethodCall(Project(v1, k1), m1, List(es1)), o, List(MethodCall(Project(v2, k2), m2, List(es2)))) =>
                  val s1 = sql.Project(translate(subst(v1,b)), k1)
                  val s2 = sql.Project(translate(subst(v2,b)), k2)
                  es1 match {
                    case Var(n) =>
                      es2 match {
                        case Var(m) =>
                          Option(List(sql.Call ("SUM", List(sql.MethodCall(sql.MethodCall(s1, m1, List(sql.Var("@"+n))), o, List(sql.MethodCall(s2, m2, List(sql.Var("@"+m)))))))))
                        case _ =>  Option(List(sql.Call ("SUM", List(sql.MethodCall(sql.MethodCall(s1, m1, List(translate(es1))), o, List(sql.MethodCall(s2, m2,  List(translate(es2)))))))))
                      }

                    case _ =>  Option(List(sql.Call ("SUM", List(sql.MethodCall(sql.MethodCall(s1, m1, List(translate(es1))), o, List(sql.MethodCall(s2, m2,  List(translate(es2)))))))))

                  }
                  //Option(List(sql.Call ("SUM", List(sql.MethodCall(sql.MethodCall(s1, m1, es1.map(translate)), m, List(sql.MethodCall(s2, m2, es2.map(translate))))))))
                case Project(v, k) => Option(List(sql.Call ("SUM", List(sql.Project(translate(subst(v, b)), k)))))
                //new
                case Var(v) => Option(List(sql.Call ("SUM", List(translate(subst(Var(v), b))))))
                case _ => Option(List(translateQualifiers("agg", qs, b)))
              })))
          , tables
          , Option(translateQualifiers("where", qs, b))
          , Option(sql.GroupBy(List(translateQualifiers("groupby", qs, b))))
        )

      case Elem(m,x)
      => sql.Elem(translate(x))
      case Merge(x, y)
      => sql.Elem(translate(y))
      case StringConst(c)
      => sql.StringConst(c)
      case IntConst(c)
      => sql.IntConst(c)
      case DoubleConst(c)
      => sql.DoubleConst(c)
      case BoolConst(c)
      => sql.BoolConst(c)

      case _ => throw new Error("Unrecongnized calculus term: "+e)
    }
}

