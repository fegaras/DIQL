import edu.uta.diql._
import org.apache.flink.api.scala._
import org.apache.flink.core.fs._

object Test {

  case class Customer ( name: String, cid: Int, account: Float )

  case class Order ( oid: Int, cid: Int, price: Float )

  def main ( args: Array[String] ) {
    val CF = args(0)
    val OF = args(1)
    val output_file = args(2)
    val env = ExecutionEnvironment.getExecutionEnvironment

    //explain(true)

    val t: Long = System.currentTimeMillis()

    val customers = env.readTextFile(CF).map{ line => line.split(",")
                          match { case Array(a,b,c) => Customer(a,b.toInt,c.toFloat) } }
    val orders = env.readTextFile(OF).map{ line => line.split(",")
                          match { case Array(a,b,c) => Order(a.toInt,b.toInt,c.toFloat) } }

    q("""
     select c.name
     from c <- customers
     where c.account < +/(select o.price from o <- orders where o.cid == c.cid)
     """).writeAsText(output_file,FileSystem.WriteMode.OVERWRITE)

    env.execute()

    println("**** DIQL Flink run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }
}
