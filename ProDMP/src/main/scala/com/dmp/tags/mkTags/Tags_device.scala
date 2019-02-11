package com.dmp.tags.mkTags

import com.dmp.`trait`.Tags
import org.apache.spark.sql.Row

object Tags_device extends Tags{
  override def makeTags(args: Any*): Map[String, Double] = {
    var map =Map[String,Double]()
    if(args.length>1){
      //强转数据
      val row: Row = args(0).asInstanceOf[Row]
      val broadcastDevice: Map[String, Double] = args(1).asInstanceOf[Map[String, Double]]
      /**
        *
        * 1##D00010001
           2##D00010002
           3##D00010003
           4##D00010004
           WIFI##D00020001
           4G##D00020002
           3G##D00020003
           2G##D00020004
           NETWORKOTHER##D00020005
          移动##D00030001
          联通##D00030002
          电信##D00030003
          OPERATOROTHER##D00030004
        * */
    }

  }
}
