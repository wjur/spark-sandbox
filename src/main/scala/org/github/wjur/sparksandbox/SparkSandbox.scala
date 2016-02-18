package org.github.wjur.sparksandbox

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object SparkSandbox {
  def main(args: Array[String]) {
    withSparkContext {
      (sparkContext, sqlContext) =>
        val data: Array[Row] = sqlContext.sql("select 1").collect()
        println(data.toSeq.mkString(", "))
    }
  }

  def withSparkContext(f: (SparkContext, SQLContext) => Unit): Unit = {
    println("Initializing Spark")
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("SparkSandbox")
      .set("spark.ui.enabled", "false")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    try {
      f(sparkContext, sqlContext)
    } finally {
      try {
        sparkContext.stop()
      }
    }
  }
}
