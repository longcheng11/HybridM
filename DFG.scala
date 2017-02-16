import org.apache.spark.SparkContext
import java.io._
import scala.collection.immutable.Map

/**
  * Created by lcheng on 2/3/2017.
  */
object DFG {

  //generate the paris in the form of [followedString,1] in the pre-processing
  def genPair(x: Array[String]): IndexedSeq[((String, String), Int)] = {
    val A = x.size - 1
    for (idx <- 1 to A - 1) yield
      ((x(idx), x(idx + 1)), 1)
  }

  def pLine(line: Iterator[String]) = {
    line.map(x => x.split(","))
  }

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "DFG", System.getenv("SPARK_HOME"))

    //val path = "hdfs://ais-hadoop-m:9100/Test/"
    val path = args(1)
    //val log = sc.textFile(path + "order.tr")
    val N = args(2).toInt
    val log = sc.textFile(path + args(3), N)
    //val m_path = "/home/lcheng/HybridM/dataset/orders/"
    val m_path = "/home/lcheng/EuroPar_17/"
    //val alpha = args(3).toFloat //the three thresholds
    //val beta = args(4).toFloat
    //val gamma = args(5).toFloat
    //val L = log.count()
    //val DEBUG = true
    //println("partition: " + N + " two thresholds: " + alpha + " " + beta)

    val attr_word = log.mapPartitions(pLine).persist()

    val pairs = attr_word.mapPartitions(iter => iter.flatMap(x => genPair(x)))

    //Array[(String, Int)]
    val stat0 = pairs.reduceByKey(_ + _).collect()

    var DFG: Map[String, Map[String, Int]] = Map()
    for (tuple <- stat0) {
      var tmp: Map[String, Int] = DFG.getOrElse(tuple._1._1, null)
      if (tmp == null) tmp = Map()
      tmp += (tuple._1._2 -> tuple._2)
      DFG += (tuple._1._1 -> tmp)
    }


    //remove (send_invoice, pay), (pay,send_invoice) and (make_delivery, confirm_payment), (confirm_payment,make_delivery)
    /*
    val a1 = "send_invoice"
    val a2 = "pay"
    val b1 = "make_delivery"
    val b2 = "confirm_payment"

    var tmp = DFG.getOrElse(a1, null)
    if (tmp != null) {
      tmp -= a2
      DFG += (a1 -> tmp)
    }

    tmp = DFG.getOrElse(a2, null)
    if (tmp != null) {
      tmp -= a1
      DFG += (a2 -> tmp)
    }

    tmp = DFG.getOrElse(b1, null)
    if (tmp != null) {
      tmp -= b2
      DFG += (b1 -> tmp)
    }

    tmp = DFG.getOrElse(b2, null)
    if (tmp != null) {
      tmp -= b1
      DFG += (b2 -> tmp)
    }*/

    val of = new File(m_path + "order.m")
    val bw = new BufferedWriter(new FileWriter(of))
    //edge and vertex

    for (line <- DFG) {
      bw.write(line._1)
      for (e <- line._2) {
        bw.write("," + e)
      }
      bw.write("\n")
    }
    bw.close()

    println("job0 is done")
    sc.stop()
  }
}
