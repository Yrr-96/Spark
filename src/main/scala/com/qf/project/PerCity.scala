package com.qf.project


import org.apache.spark.sql.{SQLContext, SparkSession}

object PerCity {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val frame = sparkSession.read.parquet("F:\\Git\\gp1923_test\\Spark\\out\\")
    frame.createTempView("table")
    val df = sparkSession.sql("select provincename,cityname,count(*) ct from table group by provincename,cityname")
    df.write.partitionBy("provincename","cityname").json("out1")
    sparkSession.stop()
  }
}
