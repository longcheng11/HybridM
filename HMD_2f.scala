import org.apache.spark.SparkContext
import scala.collection.mutable.{ListBuffer, Map, Set}
import scala.io.Source

/**
  * Created by lcheng on 11/29/2016.
  */
object HMD_2f {

  def genPair(x: Array[String]): IndexedSeq[(String, Int)] = {
    val A = x.size - 1
    for (idx <- 1 to A) yield
      (x(idx), 1)
  }

  def genMap(x: Array[String]): (String, Array[String]) = {
    val A = x.size - 1
    val aggr = new Array[String](A)
    for (idx <- 1 to A) aggr(idx - 1) = x(idx)
    (x(0), aggr)
  }

  def pLine(line: Iterator[String]) = {
    line.map(line => line.split(","))
  }

  //all the possibility with choosing m from n element
  def genFuzzyArcs(arr: Array[String], len: Int, start: Int, result: Array[String], InvertList: ListBuffer[String]): Unit = {
    if (len == 0) {
      val combine = new StringBuilder
      for (i <- result.toList.sorted) {
        combine.append(i + ".")
      }
      val invertActivity = combine.toString()
      InvertList += invertActivity

      return
    }

    for (i <- start to arr.length - len) {
      result(result.length - len) = arr(i)
      genFuzzyArcs(arr, len - 1, i + 1, result, InvertList: ListBuffer[String])
    }
  }

  def check2(line: Map[String, Int], SetA: Set[String], SetB: Set[String]): Int = {
    var countA = 0
    var countB = 0

    for (a <- SetA) countA += line.getOrElse(a, 0)
    for (b <- SetB) countB += line.getOrElse(b, 0)

    if (countA == 0 && countB == 0) 0
    else if (countA == countB) 1
    else -1
  }

  def check3(line: Array[String], SetA: Set[String], SetB: Set[String]): Int = {
    val end = line.size
    var countA = 0
    var countB = 0
    var idx = 1
    var act = ""
    while (countA >= countB && idx <= end - 1) {
      act = line(idx)
      if (SetA.contains(act)) countA += 1
      if (SetB.contains(act)) countB += 1
      idx += 1
    }

    if (countA == 0) 0 //ignore if A and B do not exist
    else if (idx == end) 1
    else -1
  }

  def check4(line: Map[String, Int], SetA: Set[String], SetB: Set[String], delta: Float): Int = {
    var numA = 0
    var numB = 0

    for (a <- SetA) numA += line.getOrElse(a, 0)
    for (b <- SetB) numB += line.getOrElse(b, 0)

    if (numA != 0 && numB != 0) {
      val threshold = {
        if (numA >= numB) numB.toFloat / numA
        else numA.toFloat / numB
      }

      if (threshold >= delta) 1
      else -1
    }
    else 0
  }

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "HMD_2f", System.getenv("SPARK_HOME"))

    //val path = "hdfs://ais-hadoop-m:9100/Test/"
    val path = args(1)
    //val log = sc.textFile(path + "test.log")
    val N = args(2).toInt
    val log = sc.textFile(path + args(3), N)
    val m_path = args(4)
    val alpha = args(5).toFloat //the three thresholds
    val beta = args(6).toFloat
    val gamma = 0.9.toFloat
    val delta = 0.9.toFloat
    //val L = log.count()
    val DEBUG = false
    //println("partition: " + N + " two thresholds: " + alpha + " " + beta)

    //read the DFG from master node
    var DFG: Map[String, Map[String, String]] = Map()
    for (line <- Source.fromFile(m_path).getLines()) {
      val sa = line.split(",")
      var tmp: Map[String, String] = Map()
      val size = sa.length
      for (i <- 1 until sa.length by 2) {
        val k = sa(i).substring(1)
        val v = sa(i + 1).substring(0, sa(i + 1).length() - 1)
        tmp += (k -> v)
      }
      DFG += (sa(0) -> tmp)
    }

    //transfer the DFG to an adjacency list
    var Graph: Map[String, Array[String]] = Map()
    for (line <- DFG) Graph += (line._1 -> line._2.keys.toArray)

    //generate the inverted-list based on the adjacency list
    var InvertList: Map[String, ListBuffer[String]] = Map()
    for (m <- Graph) {
      val size = m._2.size
      for (len <- 1 to size) {
        val r = new Array[String](len)
        val invertString = new ListBuffer[String]()
        genFuzzyArcs(m._2, len, 0, r, invertString)

        //check whether the combine in the InvertList
        for (s <- invertString) {
          var tmp = InvertList.getOrElse(s, null)
          if (tmp != null) tmp += m._1
          else tmp = ListBuffer(m._1)
          InvertList += (s -> tmp)
        }
      }
    }

    //generate all the possible conditions based on the inverted-list
    val FuzzyArcs = new ListBuffer[(Array[String], Array[String])]()
    for (line <- InvertList) {
      val B = line._1.split("\\.")
      val bSet = B.toSet

      //generate all the possibility on line._2
      val size = line._2.size
      for (len <- 1 to size) {
        val r = new Array[String](len)
        val combinedString = new ListBuffer[String]()
        genFuzzyArcs(line._2.toArray, len, 0, r, combinedString)
        for (combine <- combinedString) {
          val A = combine.split("\\.")
          val aSet = A.toSet
          if (aSet.intersect(bSet) == Set.empty) //keep the A and B has no overlap
            FuzzyArcs += ((A, B))
        }
      }
    }
    println("Number of CombinedFuzzyArcs is: " + FuzzyArcs.length)

    //process the log and cache it for reuse in memory
    val tokenActivity = log.mapPartitions(pLine).persist()

    println("------phase1-------")
    /*check 1: #LA = #LB: match at the log level*/
    //each line is a trace, tokenize the each line and get the statistic information for each activity
    val pairs = tokenActivity.mapPartitions(iter => iter.flatMap(x => genPair(x)))
    val STAT1 = pairs.reduceByKey((x, y) => x + y).collectAsMap()
    val FuzzyArcs_1 = new ListBuffer[(Array[String], Array[String])]()
    for (arc <- FuzzyArcs) {
      var numA = 0
      var numB = 0
      for (act <- arc._1) numA += STAT1.getOrElse(act, 0)
      for (act <- arc._2) numB += STAT1.getOrElse(act, 0)

      val threshold = {
        if (numA >= numB) numB.toFloat / numA
        else numA.toFloat / numB
      }

      if (threshold >= alpha) {
        FuzzyArcs_1 += arc
        if (DEBUG) {
          val s = new StringBuilder
          for (i <- arc._1) s.append(i + ".")
          s.append("|")
          for (i <- arc._2) s.append(i + ".")
          println(s)
        }
      }
    }
    println("Number of FuzzyArcs in Phase 1: " + FuzzyArcs_1.length)


    println("------phase2-------")
    /*check 2: #TA = #TB, match at the trace level*/
    //each line is a hash table to calculate the frequency of each activity
    val broadCastArcs1 = sc.broadcast(FuzzyArcs_1.map(x => (scala.collection.mutable.Set() ++ x._1, scala.collection.mutable.Set() ++ x._2))) //to mutable set
    val Trace2 = tokenActivity.mapPartitions(x => {
      for (line <- x) yield {
        val size = line.length - 1
        var y: Map[String, Int] = Map()
        for (idx <- 1 to size) {
          var count = y.getOrElse(line(idx), 0)
          count += 1
          y += (line(idx) -> count)
        }
        y
      }
    })

    val STAT2 = Trace2.mapPartitions(iter => {
      val BroFuzzyArcs = broadCastArcs1.value
      val size = BroFuzzyArcs.length
      var stat2 = Array.ofDim[Int](size, 2)
      for (line <- iter) {
        for (idx <- 0 to size - 1) {
          val arc = BroFuzzyArcs(idx)
          val CHECK = check2(line, arc._1, arc._2)
          if (CHECK == 1) stat2(idx)(0) += 1
          else if (CHECK == -1) stat2(idx)(1) += 1
        }
      }
      stat2.zipWithIndex.map { case (x, idx) => (idx, (x(0), x(1))) }.iterator
    })

    val FuzzyArcs_2 = new ListBuffer[(Array[String], Array[String])]()
    val LocStat2 = STAT2.collect.groupBy(_._1).mapValues(x => {
      var hit = 0;
      var mis = 0;
      for (tuple <- x) {
        hit += tuple._2._1
        mis += tuple._2._2
      }
      (hit, mis)
    })

    val s2 = FuzzyArcs_1.size
    for (idx <- 0 to s2 - 1) {
      val statPair = LocStat2.getOrElse(idx, null)
      if (statPair != null) {
        val ration = statPair._1.toFloat / (statPair._1 + statPair._2)
        if (ration >= beta) {
          val arc = FuzzyArcs_1(idx)
          FuzzyArcs_2 += arc
          if (DEBUG) {
            val s = new StringBuilder
            for (i <- arc._1) s.append(i + ".")
            s.append("|")
            for (i <- arc._2) s.append(i + ".")
            println(s)
          }
        }
      }
    }
    println("Number of FuzzyArcs in Phase 2: " + FuzzyArcs_2.length)

    println("------phase3-------")
    /*check 3: prefixT' #AT' = #BT', match at prefix level*/
    //broadcast the rest FuzzyArcs to all the partitions and then examine in parallel
    val broadCastArcs2 = sc.broadcast(FuzzyArcs_2.map(x => (scala.collection.mutable.Set() ++ x._1, scala.collection.mutable.Set() ++ x._2))) //to mutable set
    val STAT3 = tokenActivity.mapPartitions(iter => {
      val BroFuzzyArcs = broadCastArcs2.value
      val size = BroFuzzyArcs.length
      var stat3 = Array.ofDim[Int](size, 2)
      for (line <- iter) {
        for (idx <- 0 to size - 1) {
          val arc = BroFuzzyArcs(idx)
          val CHECK = check3(line, arc._1, arc._2)
          if (CHECK == 1) stat3(idx)(0) += 1
          else if (CHECK == -1) stat3(idx)(1) += 1
        }
      }
      stat3.zipWithIndex.map { case (x, idx) => (idx, (x(0), x(1))) }.iterator
    })

    //STAT3.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).collect
    val FuzzyArcs_3 = new ListBuffer[(Array[String], Array[String])]()
    val LocStat3 = STAT3.collect.groupBy(_._1).mapValues(x => {
      var hit = 0;
      var mis = 0;
      for (tuple <- x) {
        hit += tuple._2._1
        mis += tuple._2._2
      }
      (hit, mis)
    })

    val s3 = FuzzyArcs_2.size
    for (idx <- 0 to s3 - 1) {
      val statPair = LocStat3.getOrElse(idx, null)
      if (statPair != null && statPair._1 > 0) {
        val ration = statPair._1.toFloat / (statPair._1 + statPair._2)
        if (ration >= gamma) {
          val arc = FuzzyArcs_2(idx)
          FuzzyArcs_3 += arc
          if (DEBUG) {
            val s = new StringBuilder
            for (i <- arc._1) s.append(i + ".")
            s.append("|")
            for (i <- arc._2) s.append(i + ".")
            println(s)
          }
        }
      }
    }
    println("Number of FuzzyArcs in Phase 3: " + FuzzyArcs_3.length)


    println("------phase4-------")
    val broadCastArcs3 = sc.broadcast(FuzzyArcs_3.map(x => (scala.collection.mutable.Set() ++ x._1, scala.collection.mutable.Set() ++ x._2))) //to mutable set
    val STAT4 = Trace2.mapPartitions(iter => {
      val BroFuzzyArcs = broadCastArcs3.value
      val size = BroFuzzyArcs.length
      var stat4 = Array.ofDim[Int](size, 2)
      for (line <- iter) {
        for (idx <- 0 to size - 1) {
          val arc = BroFuzzyArcs(idx)
          val CHECK = check4(line, arc._1, arc._2, delta)
          if (CHECK == 1) stat4(idx)(0) += 1
          else if (CHECK == -1) stat4(idx)(1) += 1
        }
      }
      stat4.zipWithIndex.map { case (x, idx) => (idx, (x(0), x(1))) }.iterator
    })
    val FuzzyArcs_4 = new ListBuffer[(Array[String], Array[String])]()
    val LocStat4 = STAT4.collect.groupBy(_._1).mapValues(x => {
      var hit = 0;
      var mis = 0;
      for (tuple <- x) {
        hit += tuple._2._1
        mis += tuple._2._2
      }
      (hit, mis)
    })

    val s4 = FuzzyArcs_3.size
    for (idx <- 0 to s4 - 1) {
      val statPair = LocStat4.getOrElse(idx, null)
      if (statPair != null && statPair._1 > 0) {
        if (statPair._2 < 1) {
          val arc = FuzzyArcs_3(idx)
          FuzzyArcs_4 += arc
          if (DEBUG) {
            val s = new StringBuilder
            for (i <- arc._1) s.append(i + ".")
            s.append("|")
            for (i <- arc._2) s.append(i + ".")
            println(s)
          }
        }
      }
    }
    println("Number of FuzzyArcs in Phase 4: " + FuzzyArcs_4.length)


    //remove the precise part from graph
    val g1 = Graph.map(x => (x._1, scala.collection.mutable.Set() ++ x._2))
    for ((a1, a2) <- FuzzyArcs_4) {
      for (x <- a1; y <- a2) {
        //remove (x,y) from Graph
        var tmp = g1.getOrElse(x, null)
        if (tmp != null) tmp -= y
      }
    }

    println("------start replacement-------")
    //replace the places by gateways, numbering the gateways

    var withOutMap = Map[String, Int]()
    var withInMap = Map[String, Int]()
    var c1 = 0
    var c2 = 0
    for (arcs <- FuzzyArcs_4) {
      for (out <- arcs._1) {
        if (!withOutMap.contains(out)) {
          withOutMap += (out -> c1)
          c1 += 1
        }
      }
      for (in <- arcs._2) {
        if (!withInMap.contains(in)) {
          withInMap += (in -> c2)
          c2 += 1
        }
      }
    }
    var c3 = FuzzyArcs_4.length
    println("number of AndS: " + c1 + " , number of AndJ " + c2 + " ,number of XOR " + c3)

    //in the form of [(a,AndS),XOR,(AndJ,a)], ListBuffer[(Array[(String, String)], String, Array[(String, String)])]
    val ReplaceGW = FuzzyArcs_4.zipWithIndex.map { case (x, idx) =>
      val e1 = x._1.map(y => (y, "AndS" + withOutMap.getOrElse(y, -1)))
      val e2 = "XOR" + idx
      val e3 = x._2.map(z => (("AndJ" + withInMap.getOrElse(z, -1)), z))
      (e1, e2, e3)
    }

    if (DEBUG) {
      println("------debug 0: replaced with gateway-------")
      for ((e1, e2, e3) <- ReplaceGW) {
        for (e <- e1) print(e)
        print("\t" + e2 + "\t")
        for (e <- e3) print(e)
        println()
      }
    }

    //detect the Rule conditions
    val pCheck = new PatternCheck(ReplaceGW)
    val g2 = pCheck.run()

    //visualization
    val visual = new Visualization(g1, g2, DFG)
    visual.dotFile("1.dot")


    println("------job is done-------")
    sc.stop()
  }

}
