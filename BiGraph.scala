import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Stack
import scala.util.control.Breaks._

/**
  * Created by Long Cheng at AIS@TU/e on 9/29/2016.
  * This is use to build the Bipartite Graph and compute the Processing Instance
  */

class BiGraph(nodes: ListBuffer[Set[String]]) {
  //number of big nodes
  private val s = nodes.size

  //adjacency list based on the idx of a big node
  private var ad_list: Map[Int, Set[Int]] = Map()

  //for final process instance, the int is the idx of the Set[String] in nodes
  private type PI = Stack[Int]
  private var pi_list = new ListBuffer[PI]()

  //visited node during searching
  private var visited: Set[Int] = Set()

  //number of unique msg
  private val msgSize = {
    var uniq: Set[String] = Set()
    for (n <- nodes)
      for (m <- n)
        uniq += m
    uniq.size
  }

  def getPI(): ListBuffer[Set[String]] = {
    var p_list = new ListBuffer[Set[String]]()
    for (st <- pi_list) {
      var pi = Set[String]()
      for (idx <- st) {
        for (event <- nodes(idx)) {
          pi += event
        }
      }
      p_list += pi
    }
    p_list
  }

  //for the PI_ration condition 3, the value 0.05 is based on the suggestion of the author of the TSC paper
  def check3(beta: Float): Boolean = {
    createMap()
    computing_all()
    val pi_ration = pi_list.size.toFloat / msgSize
    if (pi_ration < beta && pi_ration > 0.05) //the threshold of pi_imbalance
      true
    else
      false
  }

  //if required, print out the real instances, only run after check3()
  def piPrint() = {
    for (st <- pi_list) {
      for (idx <- st) {
        print("[")
        for (msg <- nodes(idx))
          print(msg + ",")
        print("]\t")
      }
      print("\n")
    }
  }


  //the first step is to create the adjacency list for the inputs use a map
  def createMap() = {
    //only check the left side of the graph
    for (i <- 0 to s - 1 by 2) {
      for (j <- 1 to s - 1 by 2) {
        if (j == i + 1) {
          ad_list += (i -> Set(j))
          ad_list += (j -> Set(i))
        }
        else if (nodes(i).intersect(nodes(j)) != Set.empty) {
          //for i
          var tmp = ad_list.getOrElse(i, null)
          if (tmp != null)
            tmp += j
          else
            tmp = Set(j)
          ad_list += (i -> tmp)

          //for j
          tmp = ad_list.getOrElse(j, null)
          if (tmp != null)
            tmp += i
          else
            tmp = Set(i)
          ad_list += (j -> tmp)
        }
      }
    }
  }

  //the second step is to compute the process instance
  def computing_all() = {
    for (i <- 0 to s - 1 if !visited.contains(i)) {
      var pi = computing_1(i)
      pi_list += pi
    }
  }

  //compute the instance starting with node i, note that node i has not been visited yet
  def computing_1(i: Int): PI = {
    var node = i
    visited += node
    val node_stack = Stack[Int](node)

    //get the unvisited neighbour node
    while (node != -1) {
      val nei_nodes = ad_list.getOrElse(node, null) //nei_nodes actually can not be null, thus do not need IF here
      var new_node: Int = -1
      breakable {
        for (nu <- nei_nodes) {
          if (!visited.contains(nu)) {
            new_node = nu
            break
          }
        }
      }

      if (new_node != -1) {
        node_stack.push(new_node)
        visited += new_node
      }

      node = new_node
    }

    node_stack
  }

}
