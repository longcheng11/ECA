import java.io._
import org.apache.spark.{HashPartitioner, SparkContext}
import scala.collection.mutable.{ListBuffer, Map}
import scala.io.Source
import scala.sys.process._

/**
  * Created by lcheng on 11/17/2016.
  */
object F_Graph {

  //for the shared_ration
  def check2(as: Array[(Int, Set[String])], i: Int, j: Int, alpha: Float): Boolean = {
    val max = Math.max(as(i)._2.size, as(j)._2.size)
    val share_ration = as(i)._2.intersect(as(j)._2).size.toFloat / max
    if (share_ration >= alpha && share_ration != 1)
      true
    else
      false
  }

  //each line is in the form of an Array
  def pLine(line: Iterator[String]) = {
    line.map(line => line.split("\t"))
  }

  //generate the paris in the form of [att_idx,att_value] in the pre-processing
  def genPair(x: Array[String]): IndexedSeq[(Int, String)] = {
    val A = x.size - 1
    for (idx <- 1 to A) yield
      (idx, x(idx))
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "F_Graph", System.getenv("SPARK_HOME"))

    //val path = "hdfs://ais-hadoop-m:9100/Test/"
    val path = args(1)
    //the number of cores
    val N = args(2).toInt
    //val log = sc.textFile(path + "test")
    val log = sc.textFile(path + args(3), N)
    val alpha = args(4).toFloat //the two thresholds
    val beta = args(5).toFloat
    val L = log.count()
    println("partition: " + N + " two thresholds: " + alpha + " " + beta)

    //tokenize the each line and mark the attributes with numbers
    val attr_word = log.mapPartitions(pLine).persist()

    //get all the (i,att) pairs
    val pairs = attr_word.mapPartitions(iter => iter.flatMap(x => genPair(x)))

    //to get to get A1=[(c1,3),(c2,10),(c3,100),...] ...
    //val str_set = pairs.combineByKey(
    // v => Map(v -> 1),
    //  (x: Map[String, Int], v: String) => {
    //    val value = x.getOrElse(v, 0)
    //    if (value != 0)
    //      x + (v -> (value + 1))
    //    else
    //      x + (v -> 1)
    //   },
    //  (x: Map[String, Int], y: Map[String, Int]) => x ++ y.map { case (k, v) => k -> (v + x.getOrElse(k, 0)) }
    // )

    //val str_set = pairs.groupByKey().mapValues(x => x.groupBy(identity).mapValues(._size))

    val str_set = pairs.partitionBy(new HashPartitioner(N)).mapPartitions(x => {
      val y: Map[Int, Map[String, Int]] = Map()
      for (v <- x) {
        var mp = y.getOrElse(v._1, null)
        if (mp != null) {
          val count = mp.getOrElse(v._2, 0)
          if (count != 0)
            mp += (v._2 -> (count + 1))
          else
            mp += (v._2 -> 1)
        } else {
          mp = Map[String, Int]()
          mp += (v._2 -> 1)
        }
        y += (v._1 -> mp)
      }
      y.iterator
    })

    //implement the condition 1, distinct(A)=dis(A)/noNull(A), get the key-based rules
    val key_based = str_set.filter { case (k, v) =>
      val tmp = v.size.toFloat / v.foldLeft(0)(_ + _._2)
      tmp >= alpha && tmp != 1
    }.map(x => x._1).collect()

    //println("FilterGraph, number of key-based rules is: " + key_based.length)

    //get the unique attributes and their numbers
    //val attr_strings = str_set.mapValues(v => v.keys)
    val attr_strings = str_set.mapValues(v => v.keys.toSet)

    //an alternative way is sort the attributed and use string similarity to speedup the following comparison.
    //val attr_strings = use_strings.mapValues(v => (v.keys.toSeq.sorted, v.size))

    //pruning based on the condition 2: shared_ration= |a inter b|/max(a,b), processed on the Master
    val as = attr_strings.collect()
    val idx = as.length - 1

    //collect the adjacency list of each node based on condition 2
    val conn_pairs = new ListBuffer[(Int, Int)]()
    for (i <- 0 to idx) {
      for (j <- (i + 1) to idx)
        if (check2(as, i, j, alpha)) {
          conn_pairs += ((as(i)._1, as(j)._1))
          conn_pairs += ((as(j)._1, as(i)._1))
        }
    }

    //create alias for attributions, to 1,2,...
    var att_alias: Map[Int, Int] = Map()
    var count = 1;
    for (i <- conn_pairs) {
      if (!att_alias.contains(i._1)) {
        att_alias += (i._1 -> count)
        count += 1
      }
    }
    //debug
    //for (i <- att_alias) println("alias: " + i._1 + " " + i._2)

    //alias back
    val att_alias_back: Map[Int, Int] = att_alias.map(x => (x._2 -> x._1))

    //output the attribute alias mapping
    //val of1 = new File("alias.dat")
    //val bw1 = new BufferedWriter(new FileWriter(of1))
    //att_alias.foreach {
    // kv => bw1.write(kv._1 + "\t" + kv._2 + "\n")
    //}
    //bw1.close()

    //encoding the connect attributions with alias
    val conn_alias = conn_pairs.map(x => (att_alias.getOrElse(x._1, 0), att_alias.getOrElse(x._2, 0)))
    //for (i <- conn_alias) println(i)
    val adj_list = conn_alias.groupBy(_._1).mapValues(x => x.map(v => v._2))
    val n_vertex = adj_list.size
    val n_edge = conn_pairs.size / 2


    //alias destination
    var alias_des: Map[Int, Set[Int]] = Map()
    //alias AC partitioning based on graph partitioning
    val ac_alias = new Array[ListBuffer[(Int, Int)]](N).map(x => new ListBuffer[(Int, Int)]())
    if (n_edge <= 2 * N) {
      //assign alias_node and edges directly
      count = 0
      for (edge <- conn_alias) {
        if (edge._1 < edge._2) {
          ac_alias(count) += edge
          //add edge._1 and ._2 to alias_des
          var tmp_des: Set[Int] = alias_des.getOrElse(edge._1, null)
          if (tmp_des == null) tmp_des = Set(count)
          else tmp_des += count
          alias_des += (edge._1 -> tmp_des)

          tmp_des = alias_des.getOrElse(edge._2, null)
          if (tmp_des == null) tmp_des = Set(count)
          else tmp_des += count
          alias_des += (edge._2 -> tmp_des)

          count += 1
          count = count % N
        }
      }
    } else {
      //use metis
      //output the graph file for Metis
      var part_result: Map[Int, Int] = Map()

      val of = new File("graph.dat")
      val bw = new BufferedWriter(new FileWriter(of))
      //edge and vertex
      bw.write("\t" + n_vertex + "\t" + n_edge + "\n")

      for (i <- 1 to n_vertex) {
        adj_list.getOrElse(i, ListBuffer.empty).foreach { x =>
          bw.write("\t" + x)
        }
        bw.write("\n")
      }
      bw.close()

      //using Metis to partition the graph
      val command = "gpmetis graph.dat " + N
      command !

      //read the partitioning results
      count = 1
      for (line <- Source.fromFile("graph.dat.part." + N).getLines()) {
        part_result += (count -> line.toInt)
        count += 1
      }

      //get the partitioning strategy for the alias based on the adjacency list
      //treat the graph as a directly graph and assign based on source node to avoid duplicated assignment, A_i -> A_j for i < j
      adj_list.foreach { x =>
        var tem_des: Set[Int] = Set()
        val des_ver = part_result.getOrElse(x._1, 0) //destination of a vertex
        tem_des += des_ver
        x._2.foreach { y =>
          if (y < x._1) {
            val des_nei = part_result.getOrElse(y, -1) //destination of a followed neighbour node
            tem_des += des_nei
          }
        }
        alias_des += (x._1 -> tem_des)
      }

      //debug alias_des
      //for (i <- alias_des) println("alias_des: " + i._1 + " -> " + i._2)
      for (p <- conn_alias) {
        if (p._1 < p._2) {
          val des = part_result.getOrElse(p._1, -1)
          if (des != -1)
            ac_alias(des) += ((p._1, p._2))
          else
            println("Error with atom_alias")
        }
      }
    } //end else on graph partitioning

    //debug alias_ac
    //for (i <- ac_alias) {
    //for (p <- i) println("ac_alias: " + p._1 + ", " + p._2)
    //}

    //decoding the final partitioning results for each attribute, att->des_list
    var final_des = alias_des.map(x => (att_alias_back.getOrElse(x._1, 0), x._2))

    //decoding the final partitioning results for Atom condition on each partitioning
    val decode_ac = ac_alias.map(x => x.map(y => (att_alias_back.getOrElse(y._1, 0), att_alias_back.getOrElse(y._2, 0))))

    //add the key-based pairs to balance the computation
    for (key <- key_based) {
      val des = final_des.getOrElse(key, null)
      if (des == null) {
        //add key to the smallest partition
        var s = decode_ac(0).size
        var idx = 0
        for (j <- 1 until decode_ac.length) {
          if (s > decode_ac(j).size) {
            s = decode_ac(j).size
            idx = j
          }
        }
        //assign the key-condition to idx
        final_des += (key -> Set(idx))
        decode_ac(idx) += ((key, key))
      } else {
        //the location has been assign for the key
        for (idx <- des)
          decode_ac(idx) += ((key, key))
      }
    }

    //debug
    //for (i <- final_des) {
    // println(" dg0: " + i._1 + " -> " + i._2)
    //}

    //for (list <- decode_ac) {
    //for (p <- list) println(" dg1: " + p._1 + "," + p._2)
    //}

    //remove the tmp files
    //val command_rm = "rm -fr graph.dat*"
    //command_rm !

    //broadcast the redistribution plan to each partition
    val broadCast_Des = sc.broadcast(final_des)

    //broadcast the ACs to each partition
    val broadCast_AC = sc.broadcast(decode_ac)

    //start to partition messages based on redistribution plan
    val co_pairs = attr_word.mapPartitions(iter => {
      val m = broadCast_Des.value
      for {x <- iter
           idx <- 1 to x.size - 1
           des_list = m.getOrElse(idx, Set.empty) if (des_list != Set.empty)
           des <- des_list
      } yield
        (des, (x(idx), idx, x(0)))
    })

    println("F_Graph, number of generated pairs is: " + co_pairs.count())

    val LCBs = co_pairs.partitionBy(new HashPartitioner(N)).mapPartitions(iter => {
      val Line_LCB: Map[Int, ListBuffer[(String, Int, String)]] = Map()
      for (msgPair <- iter) {
        var tmp = Line_LCB.getOrElse(msgPair._1, null)
        if (tmp != null)
          tmp += msgPair._2
        else
          tmp = ListBuffer(msgPair._2)
        Line_LCB += (msgPair._1 -> tmp)
      }
      val LCB = Line_LCB.mapValues(iter_msg => {
        var mp: Map[String, ListBuffer[(Int, String)]] = Map()
        for (tp <- iter_msg) {
          //aggregation based on attribute values
          var tmp = mp.getOrElse(tp._1, null)
          if (tmp != null)
            tmp += ((tp._2, tp._3))
          else
            tmp = ListBuffer((tp._2, tp._3))
          mp += (tp._1 -> tmp)
        }
        for (i <- mp) yield {
          //group the value based one idx of attribute
          var final_mp: Map[Int, Set[String]] = Map() //the map of attributed_idx -> Set[msg]
          for (p <- i._2) {
            var tmp = final_mp.getOrElse(p._1, null)
            if (tmp != null)
              tmp += (p._2)
            else
              tmp = Set(p._2)
            final_mp += (p._1 -> tmp)
          }
          final_mp
        }
      })
      LCB.iterator
    })

    //check PI
    val final_AC = LCBs.mapPartitions {
      iter => {
        val final_ac = new ListBuffer[(Int, Int)]()
        for (iter_part <- iter) {
          for (ac <- broadCast_AC.value(iter_part._1)) {
            if (ac._1 == ac._2) {
              var s = 0
              for (mp <- iter_part._2) {
                if (mp.contains(ac._1)) s += 1
              }
              val pi_ration = s.toFloat / L
              if (pi_ration < beta && pi_ration > 0.05)
                final_ac += ac
            } else {
              //for each rule
              var node_list = new ListBuffer[Set[String]]() // a list of big nodes
              for (mp <- iter_part._2) {
                var tmp_1 = mp.getOrElse(ac._1, null)
                var tmp_2 = mp.getOrElse(ac._2, null)
                if (tmp_1 != null && tmp_2 != null) {
                  node_list += tmp_1
                  node_list += tmp_2
                }
              }
              //computing the PI_ration
              val biGraph = new BiGraph(node_list)
              if (biGraph.check3(beta)) {
                final_ac += ((ac._1, ac._2))
                //output += ((ac._1 + "_" + ac._2, biGraph.getPI()))
              }
            }
          }
        }
        final_ac.iterator
        //output.iterator
      }
    }

    //for (x <- final_AC.collect()) println(x)

    println("F_Graph, number of correlated rules is: " + final_AC.count())

    sc.stop()
  }
}
