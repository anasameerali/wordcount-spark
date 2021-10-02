package com.learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
case  class Words(word:String){}

object WordCountUsingDF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("wordCount")
      .getOrCreate()
    import spark.implicits._
    val rddLines  = spark.sparkContext.textFile("D:\\files\\story.txt")
    val  words = rddLines.flatMap(line =>line.split(" "))
    val  pairs = words.map(word =>(word.replaceAll("[^a-zA-Z0-9]", " ").trim.toLowerCase))
    val t =pairs.map(x=>Words(x))
     t.toDF().groupBy($"word").agg(count("word").alias("word_count")).show(false)
  }

}
