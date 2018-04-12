package com.imooc.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = SparkContext.getOrCreate(sparkConf)
    val list = Array(1,2,3,4)
    val rdd = sc.parallelize(list)

    rdd.map( i =>(i*i))
  }
}
