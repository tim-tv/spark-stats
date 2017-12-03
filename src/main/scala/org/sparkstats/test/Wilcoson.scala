package org.sparkstats.test

import breeze.numerics.{abs, signum, sqrt}
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.sparkstats.WilcosonTestResult

private[sparkstats] object Wilcoson {


  /**
    * TODO: use https://github.com/amplab/spark-indexedrdd.
    */
  implicit def bool2double(n: Boolean) = if(n) 1.0 else 0.0
  def indexed[T]: RDD[T] => RDD[(Long, T)] = rdd => rdd.zipWithIndex.map { case (k, v) => (v, k) }

  def partial_sum[T <: scala.AnyVal](rdd: RDD[(T, Int)], sc: SparkContext): RDD[(T, Int)] = {

    val partials: RDD[(Seq[(T, Int)], Int)] = rdd.mapPartitionsWithIndex((i, iter) => {
      val (keys, values) = iter.toSeq.unzip
      val sums = values.scanLeft(0)(_ + _)
      Iterator((keys.zip(sums.tail), sums.last))
    })

    val partialSums = partials.values.collect

    val sumMap = sc.broadcast(
      (0 until rdd.partitions.size)
        .zip(partialSums.scanLeft(0)(_ + _))
        .toMap
    )

    partials.keys.mapPartitionsWithIndex((i, iter) => {
      val offset = sumMap.value(i)
      if (iter.isEmpty) Iterator()
      else {
        iter.next.map { case (k, v) => (k, v + offset) }.toIterator
      }
    })
  }


  def rankdata(data: RDD[Double], sc: SparkContext): RDD[Double] = {
    val sorted = data.zipWithIndex.sortByKey().values
    val inv = sorted.zipWithIndex.sortByKey().map { case (k, v) => (v, k) }

    val i_arr = data.zipWithIndex.map { case (k, v) => (v, k) }
    val i_sorted = sorted.zipWithIndex
    val arr = i_sorted.join(i_arr).values.sortByKey().values

    val arr_tail = arr.mapPartitionsWithIndex { (id, iter) => {
      if (id == 0) (0.0 +: iter.toSeq.tail).iterator
      else iter
    }
    }

    val numb_part = arr.partitions.length
    val arr_init = arr.mapPartitionsWithIndex((id, iter) => {
      if (id == numb_part - 1) iter.toSeq.init.iterator
      else if (id == 0) (1.0 +: iter.toSeq).iterator
      else iter
    })

    val res: RDD[(Long, Int)] = indexed(arr_tail).join(indexed(arr_init)).map { case (idx, (val1, val2)) => (idx, (val1 != val2).toInt) }.sortByKey()

    val obs = partial_sum(res, sc)
    val dense = inv.join(obs).values.sortByKey().values.map(x => x.toLong)

    val len_obs = obs.count()
    val num_part = dense.partitions.size
    val count = dense.mapPartitionsWithIndex((i, iter) => {
      if (i != num_part - 1) iter.filter(_ != 0)
      else (iter.filter(_ != 0).toSeq :+ len_obs).iterator
    })

    val dense_1 = dense.map(x => x - 1)

    val i_dense = dense.zipWithIndex
    val i_dense_1 = dense_1.zipWithIndex
    val i_count: RDD[(Long, Long)] = count.zipWithIndex.map { case (k: Long, v: Long) => (v, k) }

    val i_join: RDD[(Long, (Long, Long))] = i_dense.join(i_count)
    val i_join_1: RDD[(Long, (Long, Long))] = i_dense_1.join(i_count)

    i_join.values.join(i_join_1.values).map { case (a, (x, y)) => (a, abs(x + y + 1) * 0.5) }.sortByKey().values
  }

  def testOneSample(data: RDD[Double],sc: SparkContext, correction:Boolean = false): WilcosonTestResult = {
    //    val d = data.
    object NullHypothesis extends Enumeration {
      type NullHypothesis = Value
      val OneSampleTwoSided = Value("med X = 0")
    }

    val d = data.filter(_ != 0)
    val count = d.count()

    val r = indexed(rankdata(d,sc))

    val r_plus = indexed(d.map(x=>x>0)).
      join(r).map{case (k,(v1,v2)) => (k,v1*v2)}.
      values.sum()
    val r_minus = indexed(d.map(x=>x<0)).join(r).
      map{case (k,(v1,v2)) => (k,v1*v2)}.
      values.sum()

    val T = if (r_minus<r_plus) r_minus else r_plus
    val mn = count*(count+1.0)*0.25

    var se = count * (count + 1.0) * (2.0 * count + 1.0) - r.map(elem=>(elem,1)).reduceByKey(_+_).
      filter{case (k,v) => v>1}.values.
      map(r=>0.5*(r*(r*r-1))).sum()

    se = sqrt(se / 24)

    val local_correction = 0.5*correction * signum(T-mn)
    val z = (T - mn - local_correction)/se
    val prob =2*(1-(new NormalDistribution).cumulativeProbability(z))

    new WilcosonTestResult(T,prob,NullHypothesis.OneSampleTwoSided.toString)
  }
}
