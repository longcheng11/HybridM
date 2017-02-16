import java.io._

import scala.collection.mutable.{Map, Set, Stack}

/**
  * Created by lcheng on 1/15/2017.
  */
class Visualization(black: Map[String, Set[String]], purple: Map[String, Set[String]], DFG: Map[String, Map[String, String]]) {

  private var g1_inlist: Map[String, Set[String]] = Map()

  def gw_type(node: String): Int = {
    if (node.startsWith("And")) 1
    else if (node.startsWith("XOR")) 0
    else -1 //an activity
  }

  //reverse list of g1
  def create_inList() = {
    g1_inlist = Map()
    for ((k, vSet) <- purple) {
      for (v <- vSet) {
        var in_v = g1_inlist.getOrElse(v, null)
        if (in_v == null) g1_inlist += (v -> Set(k))
        else in_v += k
      }
    }
  }

  //using DFS to get the start and end activities, and then get the number information from DFG
  def getLabel(k: String, v: String): String = {
    var count: Int = 0;
    var starts: Set[String] = Set()
    var ends: Set[String] = Set()

    //the end activities
    if (gw_type(v) == -1) ends += v
    else {
      val stack_f = new Stack[String]
      val visited: Set[String] = Set()
      stack_f.push(v)
      while (stack_f.size != 0) {
        var node = stack_f.pop()
        if (!visited.contains(node)) {
          visited += node;
          val neighs = purple.getOrElse(node, null)
          for {ele <- neighs if (!visited.contains(ele))} {
            if (gw_type(ele) == -1) {
              ends += ele
              visited += ele
            }
            else stack_f.push(ele)
          }
        }
      }
    }

    //the start activities
    if (gw_type(k) == -1) starts += k
    else {
      //first backward searching to get the starting activities using g1_inlist
      val stack_b = new Stack[String]
      val visited: Set[String] = Set()
      stack_b.push(k)
      while (stack_b.size != 0) {
        var node = stack_b.pop()
        if (!visited.contains(node)) {
          visited += node;
          val neighs = g1_inlist.getOrElse(node, null)
          for {ele <- neighs if (!visited.contains(ele))} {
            if (gw_type(ele) == -1) {
              starts += ele
              visited += ele
            }
            else stack_b.push(ele)
          }
        }
      }
    }

    //calculate the numbers based on starts and ends over DFG
    for (s <- starts; e <- ends)
      count += DFG.getOrElse(s, Map()).getOrElse(e, "0").toInt

    //return the final number
    count.toString;
  }


  def dotFile(file: String) = {
    create_inList()
    val of = new File(file)
    val bw = new BufferedWriter(new FileWriter(of))

    bw.write("digraph G { node [style=filled,shape=box];\n")

    //define the node in GraphViz
    var marked: Set[String] = Set()
    for ((k, vSet) <- purple) {
      var tp = gw_type(k)
      if (tp != -1 && !marked.contains(k)) {
        if (tp == 1) {
          bw.write(k + " [shape=none,label=\"\",image=\"/home/lcheng/HybridM/visual/and.png\"];\n")
          marked += k
        }
        else if (tp == 0) {
          bw.write(k + " [shape=none,label=\"\",image=\"/home/lcheng/HybridM/visual/xor.png\"];\n")
          marked += k
        }
      }

      for (v <- vSet) {
        tp = gw_type(v)
        if (tp != -1 && !marked.contains(v)) {
          if (tp == 1) {
            bw.write(v + " [shape=none,label=\"\",image=\"/home/lcheng/HybridM/visual/and.png\"];\n")
            marked += v
          }
          else if (tp == 0) {
            bw.write(v + " [shape=none,label=\"\",image=\"/home/lcheng/HybridM/visual/xor.png\"];\n")
            marked += v
          }
        }
      }
    }

    //for the precise arcs
    bw.write("edge [color=purple];\n")
    for ((k, vSet) <- purple; v <- vSet) {
      val num = getLabel(k, v)
      bw.write(k + " -> " + v + "[label=\"" + num + "\"];\n")
    }

    //output fuzzy part
    bw.write("edge [color=black];\n")
    for ((k, vSet) <- black; v <- vSet) {
      val num = DFG.getOrElse(k, Map()).getOrElse(v, null)
      bw.write(k + " -> " + v + "[label=\"" + num + "\"];\n")
    }
    bw.write("}\n")

    bw.close()
  }

}
