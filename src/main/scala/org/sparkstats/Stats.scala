package org.sparkstats

import org.apache.spark.rdd.RDD

/**
  * API that provides extended statistical methods for <code>Spark MLlib</code>.
  */
object Stats {

  /**
    * Prints the most awesome statistical result you have ever seen.
    *
    * @param X some magic variable
    */
  def helloWorldStat(X: RDD[Double]): Unit = println(s"The given RDD is $X")

}
