package org.apache.spark.mllib.recommendation

import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._

import scala.collection.mutable

/**
  * Created by seawalker on 2016/11/9.
  */
object EnhancedMatrixFactorizationModel {

  /**
    *
    * @param rank model.rank
    * @param srcFeatures src
    * @param dstFeatures dst
    * @param num the need K
    * @param blockSize matrix tuning
    * @return topK(product, score)
    */
  def recommendTForS(rank:Int,
                     srcFeatures:RDD[(Int, Array[Double])],
                     dstFeatures:RDD[(Int, Array[Double])],
                     num: Int,
                     blockSize:Int = 4096): RDD[(Int, Array[(Int, Double)])] = {

    val srcBlocks = blockify(rank, srcFeatures, blockSize)
    val dstBlocks = blockify(rank, dstFeatures, blockSize)

    val ratings = srcBlocks.cartesian(dstBlocks).flatMap {
        case ((srcIds, srcFactors), (dstIds, dstFactors)) =>
          val m = srcIds.length
          val n = dstIds.length
          val ratings = srcFactors.transpose.multiply(dstFactors)
          val output = new Array[(Int, (Int, Double))](m * n)
          var k = 0
          ratings foreachActive { (i, j, r) =>
            output(k) = (srcIds(i), (dstIds(j), r))
            k += 1
          }
          output.toSeq
    }
    ratings.topByKey(num)(Ordering.by(_._2))
  }


  /**
    *
    * @param model MatrixFactorizationModel by ALS.
    * @param num the need K
    * @param blockSize matrix tuning
    * @return topK(product, score)
    */
  def recommendProductsForUsers(model:MatrixFactorizationModel,
                                num: Int,
                                p1:Int = 60,
                                p2:Int = 60,
                                blockSize:Int = 4096): RDD[(Int, Array[(Int, Double)])] = {

    val rank = model.rank
    val srcFeatures = model.userFeatures.repartition(p1).cache()
    val dstFeatures = model.productFeatures.repartition(p2).cache()

    val srcBlocks = blockify(rank, srcFeatures, blockSize)
    val dstBlocks = blockify(rank, dstFeatures, blockSize)

    val ratings = srcBlocks.cartesian(dstBlocks).flatMap {
      case ((srcIds, srcFactors), (dstIds, dstFactors)) =>
        val m = srcIds.length
        val n = dstIds.length
        val ratings = srcFactors.transpose.multiply(dstFactors)
        val output = new Array[(Int, (Int, Double))](m * n)
        var k = 0
        ratings foreachActive { (i, j, r) =>
          output(k) = (srcIds(i), (dstIds(j), r))
          k += 1
        }
        output.toSeq
    }

    srcFeatures.unpersist()
    dstFeatures.unpersist()

    ratings.topByKey(num)(Ordering.by(_._2))
  }


  type BPQ = BoundedPriorityQueue[(Int, Double)]

  def topKOfPartition(iter:Iterator[(Int, (Int, Double))], num:Int):Iterator[(Int, (Int, Double))] = {
    iter.toSeq.groupBy(_._1).mapValues(rs => rs.map(_._2).aggregate(new BPQ(num)(Ordering.by(x => x._1)))(seqOP, combOP)).mapValues(_.toArray)
      .flatMap{
      case (user, rs) => rs.map(x => (user, x))
    }.toIterator
  }

  private def seqOP(queue:BPQ, item:(Int, Double)):BPQ = {
    queue += item
  }

  private def combOP(queue1:BPQ, queue2:BPQ):BPQ = {
    queue1 ++= queue2
  }

  /**
    *
    * @param rank model.rank
    * @param features omitted.
    * @param blockSize TODO: tune the block size
    * @return
    */
  def blockify(rank: Int, features: RDD[(Int, Array[Double])], blockSize:Int = 4096): RDD[(Array[Int], DenseMatrix)] = {
    val blockStorage = rank * blockSize
    features.mapPartitions { iter =>
      iter.grouped(blockSize).map { grouped =>
        val ids = mutable.ArrayBuilder.make[Int]
        ids.sizeHint(blockSize)
        val factors = mutable.ArrayBuilder.make[Double]
        factors.sizeHint(blockStorage)
        var i = 0
        grouped.foreach { case (id, factor) =>
          ids += id
          factors ++= factor
          i += 1
        }
        (ids.result(), new DenseMatrix(rank, i, factors.result()))
      }
    }
  }

}