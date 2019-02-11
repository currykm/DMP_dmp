package com.dmp.trading

import com.dmp.tools.{GlobalConfigUtils, ParseJson}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

/**
  * 基于经纬度去高德地图获取商圈相关的json字符串信息
  * 并调用ParseJson进行json解析
  * */

object ExtractTrading {
  def getArea(location:String):String={
    //代码发送->url->response->商圈
    //https://restapi.amap.com/v3/geocode/regeo?&location=116.310003,39.991957&key
    val client = new HttpClient
    //创建一个map集合
//    val map = new util.HashMap[String,String]()
//    map.put("location",location)
//    map.put("key",GlobalConfigUtils.key)
    val url = "https://restapi.amap.com/v3/geocode/regeo?&location="+location+"&key"+GlobalConfigUtils.key
    val method = new GetMethod(url)
    val code: Int = client.executeMethod(method)
    var temp = ""
    if(code == 200){
        val response: String = method.getResponseBodyAsString
      //解析字符串
        val trade: String = ParseJson.parse(response)
      temp=trade
    }
   temp
  }

}
