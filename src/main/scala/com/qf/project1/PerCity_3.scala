package com.qf.project1

import org.apache.spark.sql.SparkSession

/**
  * 统计省市指标
  */
object PerCity_3 {

  def main(args: Array[String]): Unit = {



    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 获取数据
    val df = spark.read.parquet("F:\\Git\\gp1923_test\\Spark\\out")
    // 注册临时视图
    df.createTempView("log")
    val df2 = spark
      .sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

    df2.write.partitionBy("provincename","cityname").json("out3")

    spark.stop()
  }
}
