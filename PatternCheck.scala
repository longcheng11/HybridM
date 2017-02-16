import scala.collection.mutable.{ListBuffer, Map, Set, Stack}
import scala.util.control.Breaks._

/**
  * Created by lcheng on 1/08/2017.
  */

class PatternCheck(ReplaceGW: ListBuffer[(Array[(String, String)], String, Array[(String, String)])]) {

  //adjacency list based on the idx of a big node
  private var ad_list: Map[String, Set[String]] = Map()
  private var ad_inlist: Map[String, Set[String]] = Map()

  //the final replacement solution
  private var replace: Map[String, String] = Map()
  private var re_counter = 0;

  def run(): Map[String, Set[String]] = {

    //initialization
    create_List()
    create_inList()

    if (false) {
      for ((k, vSet) <- ad_list) {
        println(k)
        for (v <- vSet) println("\t" + v)
        println()
      }
    }


    //check whether the replacement is empty
    while (applyRule() != 0) {
      println("----------debug 1: replace-------------")
      for ((k, v) <- replace) {
        println(k + "\t" + v)
      }

      //implementation of the reduction
      reduction()

      //set replacement to empty
      replace = Map()

      //update the ad_inlist based on the updated ad_list
      create_inList()
    }

    //return the updated ad_list for visualization
    ad_list
  }

  //create adjacency list for the input replaced graph
  def create_List() = {
    for (tuple <- ReplaceGW) {
      for (e1 <- tuple._1) {
        var tmp1 = ad_list.getOrElse(e1._1, null)
        if (tmp1 != null) tmp1 += e1._2
        else ad_list += (e1._1 -> Set(e1._2))

        tmp1 = ad_list.getOrElse(e1._2, null)
        if (tmp1 != null) tmp1 += tuple._2
        else ad_list += (e1._2 -> Set(tuple._2))
      }

      for (e3 <- tuple._3) {
        var tmp1 = ad_list.getOrElse(tuple._2, null)
        if (tmp1 != null) tmp1 += e3._1
        else ad_list += (tuple._2 -> Set(e3._1))

        tmp1 = ad_list.getOrElse(e3._1, null)
        if (tmp1 != null) tmp1 += e3._2
        else ad_list += (e3._1 -> Set(e3._2))
      }
    }
  }

  //compute inverse list
  def create_inList() = {
    ad_inlist = Map()
    for ((k, vSet) <- ad_list) {
      for (v <- vSet) {
        var in_v = ad_inlist.getOrElse(v, null)
        if (in_v == null) ad_inlist += (v -> Set(k))
        else in_v += k
      }
    }
  }


  def gw_type(node: String): Int = {
    if (node.startsWith("And")) return 1
    else if (node.startsWith("XOR")) return 0
    else return 2 //an activity
  }


  //check a node is a gw or not
  def Check_Type1(node: String): Boolean = {
    if (node.startsWith("XOR") || node.startsWith("And")) return true
    else return false
  }

  //check whether the same gw type and not an activity
  def Check_Type2(vSet: Set[String]): Boolean = {
    val gwt = gw_type(vSet.head)
    if (gwt != 2) {
      for (v <- vSet) {
        if (gwt != gw_type(v)) return false
      }
      return true
    }
    return false
  }

  //check whether the same gw type and not an activity
  def check_visit(vSet: Set[String], visited: Set[String]): Boolean = {
    for (v <- vSet) {
      if (visited.contains(v)) return false
    }
    return true
  }

  //check whether two sets are the same
  def Compare_Set(s1: Set[String], s2: Set[String]): Boolean = {
    if (s1.size == s2.size) {
      for (s <- s1) {
        if (!s2.contains(s)) return false
      }
      return true
    }
    return false
  }


  //scan the inverted list, k must be a gw, the path is vSet_H-->k-->vSet_L,
  def applyRule(): Int = {
    //visited nodes during searching
    var visited: Set[String] = Set()

    for ((k, vSet_H) <- ad_inlist if (Check_Type1(k) && !visited.contains(k))) {
      breakable {
        val vSet_L = ad_list.getOrElse(k, null)
        if (vSet_H.size == 1) {
          if (vSet_L.size == 1) {
            replace += (k -> "0") //this is a s2s case, namely k should be removed, apply R1 and R2
            visited += k
            visited += vSet_H.head
            visited += vSet_L.head
          }
        } else {
          //elements in vSet_H must have the same type of gateway, and none of them has been visited
          if (!check_visit(vSet_H, visited)) break
          if (!Check_Type2(vSet_H)) break

          val head_node = vSet_H.head
          val follow_head = ad_list.getOrElse(head_node, Set())
          if (!check_visit(follow_head, visited)) break

          //R5 and R6
          if (gw_type(k) != gw_type(head_node)) {
            //number of elements in vSet_L must be 1
            if (vSet_L.size != 1) break

            //number of elements followed by an element (such as head_node) in vSet_H must be 1
            for (h <- vSet_H) {
              if (ad_inlist.get(h).size != 1) break
            }

            //the follower of each element in head_follows must be 1
            for (f2 <- follow_head) {
              if (ad_list.get(f2).size != 1) break
            }

            //the followers of each element in vSet_H must be the same
            for (v <- vSet_H if (v != head_node)) {
              val other_follows = ad_list.getOrElse(v, Set())
              if (!Compare_Set(follow_head, other_follows)) break
            }

            //the followed set of each element in follow_head should be vSet_H
            for (v <- follow_head if (v != k)) {
              val other_followed = ad_inlist.getOrElse(v, Set())
              if (!Compare_Set(vSet_H, other_followed)) break
            }

            //all the element in vSet_H will be replaced by k, all the elements in follow_hend will be replaced by head_node
            var replace_1: String = ""
            var replace_2: String = ""
            if (gw_type(k) == 0) {
              //k is a XOR gw
              replace_1 = "XOR_Re" + re_counter.toString()
              re_counter += 1
              replace_2 = "And_Re" + re_counter.toString()
              re_counter += 1
            }
            else {
              replace_1 = "And_Re" + re_counter.toString()
              re_counter += 1
              replace_2 = "XOR_Re" + re_counter.toString()
              re_counter += 1
            }

            for (v <- vSet_H) {
              replace += (v -> replace_1)
              visited += v
              //the neighbour node of each v
              for (neighList <- ad_inlist.get(v); neigh <- neighList) visited += neigh
            }

            for (v <- follow_head) {
              replace += (v -> replace_2)
              visited += v

              //the neighbour node of each v
              for (neighList <- ad_list.get(v); neigh <- neighList) visited += neigh
            }
          }
          //R3 and R4
          else {
            //the followers of each element in vSet_H must be the same
            for (v <- vSet_H if (v != head_node)) {
              val other_follows = ad_list.getOrElse(v, Set())
              if (!Compare_Set(follow_head, other_follows)) break
            }

            //the followed set of each element in follow_head should be vSet_H
            for (v <- follow_head if (v != k)) {
              val other_followed = ad_inlist.getOrElse(v, Set())
              if (!Compare_Set(vSet_H, other_followed)) break
            }

            //all the element in vSet_H and follow_head will be replaced by k
            var replace_1: String = ""
            if (gw_type(k) == 0) {
              //k is a XOR gw
              replace_1 = "XOR_Re" + re_counter.toString()
              re_counter += 1
            }
            else {
              replace_1 = "And_Re" + re_counter.toString()
              re_counter += 1
            }

            for (v <- vSet_H) {
              replace += (v -> replace_1)
              visited += v

              //the neighbour node of each v
              for (neighList <- ad_inlist.get(v); neigh <- neighList) visited += neigh
            }
            for (v <- follow_head) {
              replace += (v -> replace_1)
              visited += v

              //the neighbour node of each v
              for (neighList <- ad_list.get(v); neigh <- neighList) visited += neigh
            }
          }
        }
      }
    }
    replace.size
  }

  //based on the replacement conditions to update the adjacent list
  def reduction() = {
    var tmp: Map[String, Set[String]] = Map()
    var path: Map[String, Set[String]] = Map()
    //if replaced by a gw, then directly change, if by null, then searching until get the final non-null
    for ((k, vSet) <- ad_list) {
      val rp_k = replace.getOrElse(k, k)

      if (rp_k == "0") {
        //must be a one-to-one mapping
        path += (k -> vSet)
      } else {
        var rp_set = tmp.getOrElse(rp_k, Set())
        for (v <- vSet) {
          var rp_v = replace.getOrElse(v, v)
          if (rp_v != "0") {
            if (rp_v != rp_k) rp_set += rp_v
          }
          else {
            //put (k,v) into path
            var tmp_path = path.getOrElse(k, Set())
            tmp_path += v
            path += (k -> tmp_path)
          }
        }
        if (rp_set.size != 0) tmp += (rp_k -> rp_set)
      }
    }


    if (!true) {
      println("----------path information-------------")
      for ((k, vSet) <- path) {
        println(k)
        for (v <- vSet) println("\t" + v)
        println()
      }
    }


    //searching a p2p PATH over path, and put it to tmp, note that the keys in path have not been replaced
    for ((k, vSet) <- path if (replace.getOrElse(k, "-1") != "0")) {
      for (v <- vSet) {
        val end = path.getOrElse(v, null)

        //put (k,end) to tmp
        var vset = tmp.getOrElse(k, Set())
        for (x <- end) vset += x
        tmp += (k -> vset)
      }
    }

    //update the ad_list
    ad_list = tmp
  }

}
