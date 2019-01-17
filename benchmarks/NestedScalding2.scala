import edu.uta.diql._
import com.twitter.scalding._

object Test extends ExecutionApp {

  case class Customer ( name: String, cid: Int, account: Float )

  case class Order ( oid: Int, cid: Int, price: Float )

    //explain(true)

    def job: Execution[Unit]
      = Execution.getArgs.flatMap {
           case args
             => val CF = args("CF")
                val OF = args("OF")
                val output_file = args("out")
                val customers = TypedPipe.from(TextLine(CF)).map{ line => line.split(",")
                                    match { case Array(a,b,c) => Customer(a,b.toInt,c.toFloat) } }
                val orders = TypedPipe.from(TextLine(OF)).map{ line => line.split(",")
                                    match { case Array(a,b,c) => Order(a.toInt,b.toInt,c.toFloat) } }
                q("""
                  select ( k, avg/c.account )
                  from c <- customers
                  where c.account < +/(select o.price from o <- orders where o.cid == c.cid
                                            && count/(select d from d <- customers where o.cid == d.cid) > 1)
                  group by k: c.account % 10
                  """).writeExecution(TypedTsv("out"))
        }

    override def main ( args: Array[String] ) {
      val t: Long = System.currentTimeMillis()
      super.main(args)
      println("**** DIQL Scalding run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    }
}
