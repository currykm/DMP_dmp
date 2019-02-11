package com.dmp.tags.mkTags

import com.dmp.`trait`.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
//todo:APP打标签
object Tags_APP extends Tags{
  override def makeTags(args: Any*): Map[String, Double] = {
var map =Map[String,Double]()
    if(args.length>1){
      val row: Row = args(0).asInstanceOf[Row]
      //广播变量
      val appDiv: Map[String, String] = args(1).asInstanceOf[Map[String, String]]
      //获取appID
      //获取appName
      val appid: String = row.getAs[String]("appid")
      val appname: String = row.getAs[String]("appname")
      //判断是否有appname
      val readAppname:Option[String]=>String={
        case Some(x)=>x
        case None=>appDiv.getOrElse("appid","appname")
      }
      val realAppName=readAppname(Some(appname))
      if(StringUtils.isNotBlank("appname" )&& !"".equals("appname")){
        map +=("APP"+realAppName -> 1)
      }

    }
    map
  }
}
