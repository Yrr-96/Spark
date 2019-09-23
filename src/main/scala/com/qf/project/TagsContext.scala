package com.qf.project

import com.qf.utils.TagUtil
import org.apache.spark.sql.SparkSession

object TagsContext {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("Tags").master("local").getOrCreate()
    import spark.implicits._


    val df = spark.read.parquet("F:\\Git\\gp1923_test\\Spark\\out")


    df.map(row=>{

      val userId = TagUtil.getOneUserId(row)

      val adList = TagsAd.makeTags(row)

      val businessList = BusinessTag.makeTags(row)
    }).rdd.foreach(println)

  }
}
