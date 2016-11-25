import org.apache.spark.{HashPartitioner, SparkContext}
import scala.collection.mutable.{ListBuffer, Map}

/**
  * Created by lcheng on 9/30/2016.
  */
object HVC {

  //generate the paris in the form of [(a_i,a_j), (att_value,tag,msg)]
  def genPair(x: Array[String]): IndexedSeq[((Int, Int), (String, Int, String))] = {
    val A = x.size - 1
    for {idx <- 1 to A
         j <- 1 to A
    } yield {
      var des = (idx, j)
      if (idx > j) des = des.swap
      (des, (x(idx), idx, x(0)))
    }
  }

  def pLine(line: Iterator[String]) = {
    line.map(line => line.split("\t")).flatMap(x => genPair(x))
  }

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "HVC", System.getenv("SPARK_HOME"))

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
    val pairs = log.mapPartitions(pLine)
    println("HVC, number of generated pairs is: " + pairs.count())

    //partition the generated pairs based on the hash value of the rule (int, int)
    val hash_pairs = pairs.partitionBy(new HashPartitioner(N))

    //build CMB on each partition
    val CMBs = hash_pairs.mapPartitions(iter => {
      //local aggregation based on rule on each partition
      val Line_CMB: Map[(Int, Int), ListBuffer[(String, Int, String)]] = Map()
      for (msgPair <- iter) {
        var tmp = Line_CMB.getOrElse(msgPair._1, null)
        if (tmp != null)
          tmp += msgPair._2
        else
          tmp = ListBuffer(msgPair._2)
        Line_CMB += (msgPair._1 -> tmp)
      }

      val CMB = Line_CMB.mapValues(iter_msg => {
        val mp: Map[String, ListBuffer[(Int, String)]] = Map()
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
          val final_mp: Map[Int, Set[String]] = Map() //the map of attributed_idx -> Set[msg]
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

      CMB.iterator
    })

    //check condition 1, 2, 3
    val final_AC = CMBs.mapPartitions {
      iter => {
        val final_ac = new ListBuffer[(Int, Int)]()
        for (cmb <- iter) {
          val rel = cmb._1
          if (rel._1 == rel._2) {
            //key-based correlation
            val size_cmb = cmb._2.size
            val dist_ration = size_cmb.toFloat / L
            //conditions 1
            if (dist_ration > alpha && dist_ration != 1) {
              //conditions 2
              val pi_ration = size_cmb.toFloat / L
              if (pi_ration < beta && pi_ration > 0.05)
                final_ac += rel
            }
          } else {
            //reference-based correlation
            var c1 = 0
            var c2 = 0
            for (mp <- cmb._2) {
              if (mp.size != 2) {
                if (mp.contains(rel._1)) c1 += 1
                if (mp.contains(rel._2)) c2 += 1
              }
            }
            val use_cmb = cmb._2.filter(x => x.size == 2)
            val size_cmb = use_cmb.size
            val dist_ration = size_cmb.toFloat / Math.max(c1, c2)
            if (dist_ration > alpha && dist_ration != 1) {
              //condition 3
              var node_list = new ListBuffer[Set[String]]() // a list of big nodes
              for (mp <- use_cmb) {
                var tmp_1 = mp.getOrElse(rel._1, null)
                var tmp_2 = mp.getOrElse(rel._2, null)
                if (tmp_1 != null && tmp_2 != null) {
                  node_list += tmp_1
                  node_list += tmp_2
                }
              }

              //computing the PI_ration
              val biGraph = new BiGraph(node_list)
              if (biGraph.check3(beta))
                final_ac += rel
            }
          } //end key/reference
        }
        final_ac.iterator
      }
    }

    //for (x <- final_AC.collect()) println(x)
    println("HVC, number of correlated rules is: " + final_AC.count())

    sc.stop()
  }
}
