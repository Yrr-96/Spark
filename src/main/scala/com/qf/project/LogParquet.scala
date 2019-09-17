package com.qf.project

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object LogParquet {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName(getClass.getName).setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    val lines: RDD[String] = sc.textFile("E:\\spark\\project_day01\\Spark用户画像分析\\2016-10-01_06_p1_invalid.1475274123982.log")
    lines.map(_.split(",",-1)).filter(x=>x.length>=85).map(arr=>{
      Row(

      )
    })
  }
}
