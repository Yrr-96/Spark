package com.qf.project1

import ch.hsr.geohash.GeoHash
import com.qf.utils.{AmapUtil, JedisConnectionPool, StringTypes, TagTrait}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


  object BusinessTag extends TagTrait{
    override def makeTags(args: Any*): List[(String, Int)] = {
      var list = List[(String,Int)]()


      val row = args(0).asInstanceOf[Row]

      if(StringTypes.toDouble(row.getAs[String]("long")) >=73
        && StringTypes.toDouble(row.getAs[String]("long")) <=136
        && StringTypes.toDouble(row.getAs[String]("lat"))>=3
        && StringTypes.toDouble(row.getAs[String]("lat"))<=53){

        val long = row.getAs[String]("long").toDouble
        val lat = row.getAs[String]("lat").toDouble

        val business = getBusiness(long,lat)
        if(StringUtils.isNoneBlank(business)){
          val str = business.split(",")
          str.foreach(str=>{
            list:+=(str,1)
          })
        }
      }
      list
    }


    def getBusiness(long:Double,lat:Double):String={

      val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)

      var business = redis_queryBusiness(geohash)

      if(business == null){
        business = AmapUtil.getBusinessFromAmap(long,lat)

        if(business!=null && business.length>0){
          redis_insertBusiness(geohash,business)
        }
      }
      business
    }


    def redis_queryBusiness(geohash:String):String={
      val jedis = JedisConnectionPool.getConnection()
      val business = jedis.get(geohash)
      jedis.close()
      business
    }


    def redis_insertBusiness(geohash:String,business:String): Unit ={
      val jedis = JedisConnectionPool.getConnection()
      jedis.set(geohash,business)
      jedis.close()
    }
  }


