package com.qf.utils

/**
  * 类型工具类
  */
object StringTypes {

  def toInt(str:String):Int={
    try{
      str.toInt
    }catch {
      case _ :Exception =>0
    }
  }

  def toDouble(str: String):Double ={
    try{
      str.toDouble
    }catch {
      case _ :Exception =>0.0
    }
  }
}
