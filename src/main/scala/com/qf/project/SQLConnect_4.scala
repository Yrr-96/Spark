package com.qf.project



import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

// 存Mysql
object SQLConnect_4 {

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



//     通过config配置文件依赖进行加载相关的配置信息
        val load = ConfigFactory.load()
        // 创建Properties对象
        val prop = new Properties()
        prop.setProperty("user",load.getString("jdbc.user"))
        prop.setProperty("password",load.getString("jdbc.password"))
        // 存储
        df2.write.mode(SaveMode.Append).jdbc(
          load.getString("jdbc.url"),load.getString("jdbc.tablName"),prop)

    spark.stop()
  }
}
