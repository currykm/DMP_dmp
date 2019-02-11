package com.dmp.tags.mkTags

import com.dmp.`trait`.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object Tags_channel  extends Tags{
  override def makeTags(args: Any*): Map[String, Double] = {
    var map = Map[String,Double]()
    if(args.length>0){
      val row: Row = args(0).asInstanceOf[Row]
      val channelid: String = row.getAs[String]("channelid")
      if(StringUtils.isNotBlank(channelid)){
        map+=("Channel"+channelid->1)
      }
    }
    map
  }
}
