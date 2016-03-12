package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) = {
    
    val conf = new SparkConf()
    .setAppName("WordCount")
    .setMaster("local")
   // create spark context
    val sc = new SparkContext(conf)
    
    val textrdd = sc.textFile("device.txt")
    
    textrdd.flatMap{ line =>
      line.split(" ")
    }
    .map { word =>
      (word.toLowerCase,1)
    }
    .reduceByKey(_ + _)
    .sortBy(_._2, false)
    .foreach(println)
    //.saveAsTextFile("C:/Users/todd/workspace/test/devicewordcount.txt")
  }
}