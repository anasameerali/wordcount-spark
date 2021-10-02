package com.learning

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object WordCountUsingRdd {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("wordCount")
      .getOrCreate()
    val rddLines  = spark.sparkContext.textFile("D:\\files\\story.txt")
    val  words = rddLines.flatMap(line =>line.split(" "))
    val  pairs = words.map(word =>(word.replaceAll("[^a-zA-Z0-9]", " ").trim.toLowerCase,1))
    pairs.reduceByKey(_+_).foreach(x => println(x._1+"  = "+x._2))
  }

}
