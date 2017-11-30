package org.sparkstats.test

import breeze.numerics.exp
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.rdd.RDD
import org.sparkstats.AndersonTestResult

import scala.math.log

private[sparkstats] object AndersonTest{
  object NullHypothesis extends Enumeration {
    type NullHypothesis = Value
    val OneSampleTwoSided = Value("Sample has a normal distribution ")
  }
  def testOneSample(data: RDD[Double]): AndersonTestResult = {

    val N = data.count()
    val ad = anderson_statistic(data,N)
    val ad2a = ad * (1 + 0.75/N + 2.25/math.pow(N,2))
    val pval = {
      if ((ad2a >= 0.00) && (ad2a < 0.200))
        1 - exp(-13.436 + 101.14 * ad2a - 223.73 * ad2a*ad2a)
      else if(ad2a < 0.340)
        exp(-8.318 + 42.796 * ad2a - 59.938 * ad2a*ad2a)
      else if(ad2a <0.600)
        1-exp(-8.318 + 42.796 * ad2a - 59.938 * ad2a*ad2a)
      else if(ad2a <= 13)
        exp(1.2937 - 5.709 * ad2a + 0.0186 * ad2a*ad2a)
      else
        0
    }
    new AndersonTestResult(ad,pval,NullHypothesis.OneSampleTwoSided.toString)
  }

  def anderson_statistic(data: RDD[Double],N:Long): Double ={

    val y: RDD[Double] = data.sortBy(key=>key).persist()

    val xbar = y.mean()
    val s = y.sampleStdev()

    val cdf=new NormalDistribution(0, 1)
    val z_a: RDD[Double] = y.map(x_ => cdf.cumulativeProbability((x_ - xbar)/s)).persist()
    val z_d = z_a.sortBy(key=>key,false).zipWithIndex

    val S = z_a.zipWithIndex.map{case (k,v)=>(v,k)}.union(z_d.map{case (k,v)=>(v,k)}).groupByKey
      .map{case(i,v)=>(2*(i+1)-1)*v.map(v=>log(v)).sum}.sum()

    -N-S
  }
}