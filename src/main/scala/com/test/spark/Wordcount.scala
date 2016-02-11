package com.test.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._
import scala.collection.JavaConversions._
object Wordcount {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
              .setAppName("word count")
              .setMaster("local")
              
              
    val sc = new SparkContext(conf)
    
    if (args.length < 2) 
    {
      println("Missing arguments")
    }
    
    val txfile = sc.textFile(args(0))
    println("####input : -" + args(0))
    
    
    val words = txfile.flatMap ( line => line.split(" ") )
    
    val counts  = words.map ( word => (word.replaceAll("	", "") , 1) )
    
    val wordcnt = counts.reduceByKey(_ + _)
    val worsort = wordcnt.sortByKey()
    worsort.foreach(println)
   
    worsort.saveAsTextFile(args(1))
    println("####output : -" + args(1))
    
              
  }
  
              
}